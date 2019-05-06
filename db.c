#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

enum MetaCommandResult_t {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND,
};

typedef enum MetaCommandResult_t MetaCommandResult;

MetaCommandResult do_meta_command(InputBuffer* input_buffer) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
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

struct Table_t {
  uint32_t num_rows;
  void* pages[MAX_TABLE_PAGE_NUM];
};
typedef struct Table_t Table;

Table* new_table() {
  Table* table = malloc(sizeof(Table));
  table->num_rows = 0;
  for (int i = 0; i < MAX_TABLE_PAGE_NUM; i++) {
    table->pages[i] = NULL;
  }

  return table;
}

void free_table(Table* table) {
  for (int i = 0; table->pages[i] != NULL; i++) {
    free(table->pages[i]);
  }
  free(table);
}

void* row_slot(Table* table, uint32_t row_num) {
  uint32_t page_idx = row_num / ROWS_PER_PAGE;
  if (page_idx >= MAX_TABLE_PAGE_NUM) {
    // TODO: Error handling
    exit(EXIT_FAILURE);
  }

  void *page = table->pages[page_idx];
  if (page == NULL) {
    table->pages[page_idx] = malloc(PAGE_SIZE);
    page = table->pages[page_idx];
  }

  uint32_t row_idx_in_page = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = ROW_SIZE * row_idx_in_page;
  return page + byte_offset;
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
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_SYNTAX_ERROR,
  PREPARE_STRING_TOO_LONG,
};
typedef enum PrepareResult_t PrepareResult;

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
  statement->type = STATEMENT_INSERT;
  char* type = strtok(input_buffer->buffer, " ");
  char* id_str = strtok(NULL, " ");
  char* username = strtok(NULL, " ");
  char* email = strtok(NULL, " ");

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

  void* slot = row_slot(table, table->num_rows);
  seriarize_row(&(statement->row_to_insert), slot);

  table->num_rows++;

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
  Row row;
  for (int i = 0; i < table->num_rows; i++) {
    void* slot = row_slot(table, i);
    deseriarize_row(slot, &row);
    print_row(&row);
  }

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

int main(int argc, char* argv[]) {
  Table* table = new_table();
  InputBuffer* input_buffer = new_input_buffer();

  while (true) {
    print_prompt();
    read_input(input_buffer);

    if(input_buffer->buffer[0] == '.') {
      switch(do_meta_command(input_buffer)) {
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
      case PREPARE_UNRECOGNIZED_STATEMENT:
        printf("Unrecognized statement: '%s'.\n", input_buffer->buffer);
        continue;
      case PREPARE_SYNTAX_ERROR:
        printf("Syntax error. Could not parse statement '%s'.\n", input_buffer->buffer);
        continue;
      case PREPARE_STRING_TOO_LONG:
        printf("String is too long.\n");
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
  free_table(table);
  exit(EXIT_SUCCESS);
}
