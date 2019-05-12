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

enum NodeType_t {
  NODE_INTERNALE,
  NODE_LEAF
};
typedef enum NodeType_t NodeType;

/*
 * Common Node Header Layout
 */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET= NODE_TYPE_OFFSET + NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * Leaf Node Header Layout
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

/*
 Leaf Node Body Layout
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

uint32_t* leaf_node_num_cells(void* node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

uint8_t* leaf_node_type(void* node) {
  return node + NODE_TYPE_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + LEAF_NODE_CELL_SIZE * cell_num;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_OFFSET;
}

void* leaf_node_value(void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_VALUE_OFFSET;
}

NodeType get_node_type(void* node) {
  return (NodeType)*leaf_node_type(node);
}

void set_node_type(void* node, NodeType type) {
  *leaf_node_type(node) = (uint8_t)type;
}

void initialize_leaf_node(void* node) {
  *leaf_node_num_cells(node) = 0;
  set_node_type(node, NODE_LEAF);
}

struct Pager_t {
  int fd;
  uint32_t file_length;
  uint32_t num_pages;
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
  pager->num_pages = file_length / PAGE_SIZE;

  if (pager->file_length % PAGE_SIZE != 0) {
    printf("DB file is not a whole number of pages. DB file is corrupted.\n");
    exit(EXIT_FAILURE);
  }

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

void pager_flush(Pager* pager, uint32_t page_num) {
  if (pager->pages[page_num] == NULL) {
    printf("Error: Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
  if (offset < 0) {
    printf("Error: failed to seek: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->fd, pager->pages[page_num], PAGE_SIZE);
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

    if (page_num < pager->num_pages) {
      // page exists in file
      lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->fd, page, PAGE_SIZE);
      if (bytes_read < 0) {
        printf("Error reading file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    } else {
      // page doesn't exist in file. let's extend page
      pager->num_pages = page_num + 1;
    }

    pager->pages[page_num] = page;
  }

  return pager->pages[page_num];
}

struct Table_t {
  Pager* pager;
  uint32_t root_page_num;
};
typedef struct Table_t Table;

Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);

  Table *table = malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = 0; // tmp value for implementing single leaf node pattern

  if (pager->num_pages == 0) {
    // New database file. Initialize page 0 as a leaf node.
    void* root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
  }

  return table;
}

void db_close(Table* table) {
  for (uint32_t i = 0; i < table->pager->num_pages; i++) {
    // Flush all pages
    if (table->pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(table->pager, i);
  }

  pager_close(table->pager);
  free(table);
}

struct Cursor_t {
  Table *table;
  uint32_t page_num;
  uint32_t cell_num;
  bool end_of_table; // Indicates a position one past the last element
};
typedef struct Cursor_t Cursor;

Cursor* table_start(Table* table) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = table->root_page_num;
  cursor->cell_num = 0;

  void* root_node = get_page(table->pager, table->root_page_num);
  uint32_t root_node_num_cells = *leaf_node_num_cells(root_node);
  cursor->end_of_table = (root_node_num_cells == 0);

  return cursor;
}

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
  void* node = get_page(table->pager, page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = page_num;

  // Binary Search
  uint32_t min_idx = 0;
  uint32_t one_past_max_idx = num_cells;
  while (min_idx < one_past_max_idx) {
    uint32_t mid_idx = min_idx + (one_past_max_idx - min_idx) / 2;
    uint32_t mid_key = *leaf_node_key(node, mid_idx);

    if (key == mid_key) {
      cursor->cell_num = mid_idx;
      return cursor;
    }

    if (key < mid_key) {
      one_past_max_idx = mid_idx;
    } else {
      min_idx = mid_idx + 1;
    }
  }

  cursor->cell_num = min_idx;
  return cursor;
}

/*
 * Return the position of the given key.
 */
Cursor* table_find(Table* table, uint32_t key) {
  uint32_t root_page_num = table->root_page_num;
  void* root_node = get_page(table->pager, root_page_num);

  if (*leaf_node_type(root_node) == NODE_LEAF) {
    return leaf_node_find(table, root_page_num, key);
  } else {
    printf("Need to implement searching an internal node\n");
    exit(EXIT_FAILURE);
  }
}

Cursor* table_end(Table* table) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = table->root_page_num;

  void* root_node = get_page(table->pager, table->root_page_num);
  cursor->cell_num = *leaf_node_num_cells(root_node);
  cursor->end_of_table = true;
  return cursor;
}

void cursor_advance(Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* node = get_page(cursor->table->pager, page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  cursor->cell_num++;
  if (cursor->cell_num >= num_cells) {
    cursor->end_of_table = true;
  }
}

void* cursor_value(Cursor* cursor) {
  void* node = get_page(cursor->table->pager, cursor->page_num);
  return leaf_node_value(node, cursor->cell_num);
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
  void* node = get_page(cursor->table->pager, cursor->page_num);

  uint32_t num_cells = *leaf_node_num_cells(node);
  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    // Node full
    printf("Need to implement splitting leaf node.\n");
    exit(EXIT_FAILURE);
  }

  if (cursor->cell_num < num_cells) {
    // Make room for new cell
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i-1), LEAF_NODE_CELL_SIZE);
    }
  }

  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_key(node, cursor->cell_num)) = key;
  seriarize_row(value, leaf_node_value(node, cursor->cell_num));
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
  EXECUTE_DUPLICATE_KEY,
  EXECUTE_TABLE_FULL,
};
typedef enum ExecuteResult_t ExecuteResult;

ExecuteResult execute_insert(Statement* statement, Table* table) {
  void* node = get_page(table->pager, table->root_page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    return EXECUTE_TABLE_FULL;
  }

  Row* row_to_insert = &(statement->row_to_insert);
  uint32_t key_to_insert = row_to_insert->id;
  Cursor* cursor = table_find(table, key_to_insert);

  if (cursor->cell_num < num_cells) {
    uint32_t key_at_cursor = *leaf_node_key(node, cursor->cell_num);
    if (key_at_cursor == key_to_insert) {
      return EXECUTE_DUPLICATE_KEY;
    }
  }

  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

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

void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void print_leaf_node(void *node) {
  uint32_t num_cells = *leaf_node_num_cells(node);
  printf("leaf (size %d)\n", num_cells);
  for (uint32_t i = 0; i < num_cells; i++) {
    uint32_t key = *leaf_node_key(node, i);
    printf(" - %d : %d\n", i, key);
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
  } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
    printf("Constants:\n");
    print_constants();
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
    printf("Tree:\n");
    print_leaf_node(get_page(table->pager, 0));
    return META_COMMAND_SUCCESS;
  }else {
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
      case EXECUTE_DUPLICATE_KEY:
        printf("Error: Duplicate key.\n");
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
