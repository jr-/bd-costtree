CC=g++
CFLAGS=-Wall -std=c++11
DEBUGFLAGS=-g -O0

TEST_LIB=-lgtest

EXEC=tree
TEST_EXEC=tree_test

SOURCE_DIR=src
HEADERS_DIR=include
TEST_DIR=tests

MAIN_FILE :=$(SOURCE_DIR)/main.cpp
SOURCE_FILES :=$(filter-out $(MAIN_FILE),$(wildcard $(SOURCE_DIR)/*.cpp))
TEST_SOURCE_FILES :=$(wildcard $(TEST_DIR)/*.cpp)


make: $(SOURCE_FILES)
	$(CC) -o $(EXEC) $(SOURCE_FILES) $(MAIN_FILE) $(CFLAGS) -I$(HEADERS_DIR)

debug: $(SOURCE_FILES)
	$(CC) -o $(EXEC) $(SOURCE_FILES) $(MAIN_FILE) $(CFLAGS) $(DEBUGFLAGS) -I$(HEADERS_DIR)

test: $(SOURCE_FILES) $(TEST_SOURCE_FILES)
	$(CC) -o $(TEST_EXEC) $(SOURCE_FILES) $(TEST_SOURCE_FILES) $(CFLAGS) $(DEBUGFLAGS) $(TEST_LIB) -I$(HEADERS_DIR)
	./$(TEST_EXEC)

clean:
	rm -f $(EXEC) $(TEST_EXEC) include/*.h.gch
