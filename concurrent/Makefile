.PHONY: all clean
.DEFAULT_GOAL := all

LIBS=-lrt -lm -lpmemobj -lpthread
INCLUDES=-I./include
CFLAGS= -std=c++17 

all: main

main: src/test.cpp
	g++ $(CFLAGS) -o bin/btree_concurrent src/test.cpp $(LIBS)
	g++ $(CFLAGS) -o bin/btree_mixed src/mixed.cpp $(LIBS)

clean: 
	rm bin/*
