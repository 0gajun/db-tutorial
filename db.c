#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct InputBuffer_t {
  char* buffer;
  size_t buffer_length;
  ssize_t input_length;
};
typedef struct InputBuffer_t InputBuffer;

InputBuffer* new_input_buffer() {
  InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

void close_input_buffer(InputBuffer* input_buffer) {
  free(input_buffer->buffer);
  free(input_buffer);
}

void print_prompt() {
  printf("db > ");
}

void read_input(InputBuffer* input_buffer) {
  ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }

  input_buffer->input_length = bytes_read -1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

struct Row_t {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
};
typedef struct Row_t Row;

void print_row(Row* row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

void seriarize_row(Row* src, void* dest) {
  memcpy(dest + ID_OFFSET, &(src->id), ID_SIZE);
  memcpy(dest + USERNAME_OFFSET, &(src->username), USERNAME_SIZE);
  memcpy(dest + EMAIL_OFFSET, &(src->email), EMAIL_SIZE);
}

void deseriarize_row(void* src, Row* dest) {
  memcpy(&(dest->id), src + ID_OFFSET, ID_SIZE);
  memcpy(&(dest->username), src + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(dest->email), src + EMAIL_OFFSET, EMAIL_SIZE);
}

const uint32_t PAGE_SIZE = 4096;
#define MAX_TABLE_PAGE_NUM 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * MAX_TABLE_PAGE_NUM;

struct Pager_t {
  int fd;
  uint32_t file_length;
  void* pages[MAX_TABLE_PAGE_NUM];
};
typedef struct Pager_t Pager;

Pager* pager_open(const char* filename) {
  int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

  if (fd == -1) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->fd = fd;
  pager->file_length = file_length;

  for (int i = 0; i < MAX_TABLE_PAGE_NUM; i++) {
    pager->pages[i] = NULL;
  }

  return pager;
}

void pager_close(Pager* pager) {
  int result = close(pager->fd);
  if (result < 0) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }

  for (int i = 0; i < MAX_TABLE_PAGE_NUM; i++) {
    if (pager->pages[i]) {
      free(pager->pages[i]);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
  if (pager->pages[page_num] == NULL) {
    printf("Error: Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
  if (offset < 0) {
    printf("Error: failed to seek: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->fd, pager->pages[page_num], size);
  if (bytes_written < 0) {
    printf("Error: flushing page: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

void* get_page(Pager* pager, uint32_t page_num) {
  if (page_num >= MAX_TABLE_PAGE_NUM) {
    printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, MAX_TABLE_PAGE_NUM);
    exit(EXIT_FAILURE);
  }

  if (pager->pages[page_num] == NULL) {
    // Cache miss
    void* page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    // We might save a partial page at the end of the file
    if (pager->file_length % PAGE_SIZE) {
      num_pages++;
    }

    if (page_num <= num_pages) {
      // page exists in file
      lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->fd, page, PAGE_SIZE);
      if (bytes_read < 0) {
        printf("Error reading file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }

    pager->pages[page_num] = page;
  }

  return pager->pages[page_num];
}

struct Table_t {
  uint32_t num_rows;
  Pager* pager;
};
typedef struct Table_t Table;

Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);
  // TODO : Recnosider following calculation
  uint32_t num_of_filled_pages = pager->file_length / PAGE_SIZE;
  uint32_t num_of_additional_rows = (pager->file_length % PAGE_SIZE) / ROW_SIZE;
  uint32_t num_of_rows = num_of_filled_pages * ROWS_PER_PAGE + num_of_additional_rows;

  Table *table = malloc(sizeof(Table));
  table->num_rows = num_of_rows;
  table->pager = pager;

  return table;
}

void db_close(Table* table) {
  uint32_t num_of_filled_pages = table->num_rows / ROWS_PER_PAGE;
  for (uint32_t i = 0; i < num_of_filled_pages; i++) {
    if (table->pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(table->pager, i, PAGE_SIZE);
  }

  uint32_t num_of_additional_rows = table->num_rows % ROWS_PER_PAGE;
  if (num_of_additional_rows > 0) {
    uint32_t last_page_num = num_of_filled_pages;
    if (table->pager->pages[last_page_num] != NULL) {
      pager_flush(table->pager, last_page_num, num_of_additional_rows * ROW_SIZE);
    }
  }

  pager_close(table->pager);
  free(table);
}

void* row_slot(Table* table, uint32_t row_num) {
  uint32_t page_idx = row_num / ROWS_PER_PAGE;
  void* page = get_page(table->pager, page_idx); // NULL is never returned from get_page

  uint32_t row_idx_in_page = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = ROW_SIZE * row_idx_in_page;
  return page + byte_offset;
}

struct Cursor_t {
  Table *table;
  uint32_t row_num;
  bool end_of_table; // Indicates a position one past the last element
};
typedef struct Cursor_t Cursor;

Cursor* table_start(Table* table) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->row_num = 0;
  cursor->end_of_table = (table->num_rows == 0);

  return cursor;
}

Cursor* table_end(Table* table) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->row_num = table->num_rows;
  cursor->end_of_table = true;
}

void cursor_advance(Cursor* cursor) {
  cursor->row_num++;
  if (cursor->row_num >= cursor->table->num_rows) {
    cursor->end_of_table = true;
  }
}

void* cursor_value(Cursor* cursor) {
  uint32_t page_idx = cursor->row_num / ROWS_PER_PAGE;
  void* page = get_page(cursor->table->pager, page_idx);

  uint32_t row_idx_in_page = cursor->row_num % ROWS_PER_PAGE;
  uint32_t row_byte_offset_in_page = ROW_SIZE * row_idx_in_page;

  return page + row_byte_offset_in_page;
}

enum StatementType_t {
  STATEMENT_INSERT,
  STATEMENT_SELECT,
};
typedef enum StatementType_t StatementType;

struct Statement_t {
  StatementType type;
  Row row_to_insert; // only used by insert statement
};
typedef struct Statement_t Statement;

enum PrepareResult_t {
  PREPARE_SUCCESS,
  PREPARE_NEGATIVE_ID,
  PREPARE_STRING_TOO_LONG,
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_SYNTAX_ERROR,
};
typedef enum PrepareResult_t PrepareResult;

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
  statement->type = STATEMENT_INSERT;
  char* type = strtok(input_buffer->buffer, " ");
  char* id_str = strtok(NULL, " ");
  char* username = strtok(NULL, " ");
  char* email = strtok(NULL, " ");

  int id = atoi(id_str);

  if (id < 0) {
    return PREPARE_NEGATIVE_ID;
  }

  if (id_str == NULL || username == NULL || email == NULL) {
    return PREPARE_SYNTAX_ERROR;
  }

  if (strlen(username) > COLUMN_USERNAME_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }

  if (strlen(email) > COLUMN_EMAIL_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }

  statement->row_to_insert.id = atoi(id_str);
  // src string's length are already validated above
  strcpy(statement->row_to_insert.username, username);
  strcpy(statement->row_to_insert.email, email);

  return PREPARE_SUCCESS;
}

PrepareResult prepare_select(InputBuffer* input_buffer, Statement* statement) {
  statement->type = STATEMENT_SELECT;
  return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, statement);
  } else if (strcmp(input_buffer->buffer, "select") == 0) {
    return prepare_select(input_buffer, statement);
  } else {
    return PREPARE_UNRECOGNIZED_STATEMENT;
  }
}

enum ExecuteResult_t {
  EXECUTE_SUCCESS,
  EXECUTE_TABLE_FULL,
};
typedef enum ExecuteResult_t ExecuteResult;

ExecuteResult execute_insert(Statement* statement, Table* table) {
  if (table->num_rows >= TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
  }

  Cursor* cursor = table_end(table);
  seriarize_row(&(statement->row_to_insert), cursor_value(cursor));

  table->num_rows++;

  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
  Row row;
  Cursor *cursor = table_start(table);
  while(!(cursor->end_of_table)) {
    deseriarize_row(cursor_value(cursor), &row);
    print_row(&row);
    cursor_advance(cursor);
  }

  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table) {
  switch (statement->type) {
    case STATEMENT_INSERT:
      return execute_insert(statement, table);
    case STATEMENT_SELECT:
      return execute_select(statement, table);
  }
}

enum MetaCommandResult_t {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND,
};

typedef enum MetaCommandResult_t MetaCommandResult;

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    db_close(table);
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Error: Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }

  char* filename = argv[1];
  Table* table = db_open(filename);
  InputBuffer* input_buffer = new_input_buffer();

  while (true) {
    print_prompt();
    read_input(input_buffer);

    if(input_buffer->buffer[0] == '.') {
      switch(do_meta_command(input_buffer, table)) {
        case META_COMMAND_SUCCESS:
          continue;
        case META_COMMAND_UNRECOGNIZED_COMMAND:
          printf("Unrecognized command: '%s'.\n", input_buffer->buffer);
          continue;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
      case PREPARE_SUCCESS:
        break;
      case PREPARE_STRING_TOO_LONG:
        printf("String is too long.\n");
        continue;
      case PREPARE_NEGATIVE_ID:
        printf("ID must be positive.\n");
        continue;
      case PREPARE_UNRECOGNIZED_STATEMENT:
        printf("Unrecognized statement: '%s'.\n", input_buffer->buffer);
        continue;
      case PREPARE_SYNTAX_ERROR:
        printf("Syntax error. Could not parse statement '%s'.\n", input_buffer->buffer);
        continue;
    }

    switch (execute_statement(&statement, table)) {
      case EXECUTE_SUCCESS:
        printf("Executed.\n");
        break;
      case EXECUTE_TABLE_FULL:
        printf("Error: Table full.\n");
        break;
    }
  }

  close_input_buffer(input_buffer);
  db_close(table);
  exit(EXIT_SUCCESS);
}
