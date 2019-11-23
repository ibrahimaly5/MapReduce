CC:=g++
WARN:=-Wall 
LIB:=-pthread -O3
CCOPTS:=-std=c++11 -ggdb -D_GNU_SOURCE
TARGET:=wc

$(TARGET): distwc.o threadpool.o mapreduce.o
	g++ -std=c++11 distwc.o threadpool.o mapreduce.o -o $(TARGET) $(LIB)

compile: threadpool.o mapreduce.o distwc.o

compress:
	tar -czvf mapreduce.tar.gz mapreduce.* threadpool.* Makefile distwc.cc README

%.o: %.cc
	$(CC) $(WARN) $(CCOPTS) $< -c $(LIB)

clean-all:
	rm -rf *.o $(TARGET)

threadpool.o: threadpool.cc threadpool.h
mapreduce.o: mapreduce.cc mapreduce.h threadpool.h
