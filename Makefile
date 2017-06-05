CC=g++
CFLAGS=-Wall -std=c++11
DEBUGFLAGS=-g
EXEC=tree

SOURCE_DIR=src
HEADERS_DIR=include

SOURCE_FILES=$(wildcard $(SOURCE_DIR)/*.cpp)

make: $(SOURCE_FILES)
	$(CC) -o $(EXEC) $(SOURCE_FILES) $(CFLAGS) -I$(HEADERS_DIR)

debug: $(SOURCE_FILES)
	$(CC) -o $(EXEC) $(SOURCE_FILES) $(CFLAGS) $(DEBUGFLAGS) -I$(HEADERS_DIR)

clean:
	rm -f $(EXEC) include/*.h.gch
