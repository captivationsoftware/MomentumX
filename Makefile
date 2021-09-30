CC = g++

CFLAGS = -std=c++11 -g -Wall -c

all: libmomentum.so

momentum.o: momentum.cpp momentum.h
	$(CC) $(CFLAGS) -fPIC -o momentum.o momentum.cpp 

libmomentum.so: momentum.o
	$(CC) -shared -o libmomentum.so momentum.o

clean:
	rm -f *.so *.o
