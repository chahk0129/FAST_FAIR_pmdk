.PHONY: all clean
.DEFAULT_GOAL := all

LIBS=-lrt -lm -lpmemobj
INCLUDES=-I ../../install/usr/local/include
CFLAGS=-O3 -std=c++11 -w
#CFLAGS=-O0 -std=c++11 -w #-g

output = btree 

all: main

main: src/test.cpp
	g++ $(CFLAGS) -o bin/btree src/test.cpp $(LIBS)
	g++ $(CFLAGS) -o bin/breakdown src/test.cpp $(LIBS) -DBREAKDOWN

clean: 
	rm bin/*
