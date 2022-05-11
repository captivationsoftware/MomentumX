CC = g++

CFLAGS = -std=c++11 -g -Wall -c -fPIC

LIBS = -lrt 

all: libmomentum.so

momentum.o: momentum.cpp momentum.h
	$(CC) $(CFLAGS) -o momentum.o momentum.cpp


libmomentum.so: momentum.o
	$(CC) -shared -o libmomentum.so momentum.o $(LIBS)

clean:
	rm -f *.so *.o
