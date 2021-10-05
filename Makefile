CC = g++

CFLAGS = -std=c++11 -g -Wall -c 

LIBS = -lrt -lzmq

all: libmomentum.so

momentum.o: momentum.cpp momentum.h
	$(CC) $(CFLAGS) -fPIC -o momentum.o momentum.cpp


libmomentum.so: momentum.o
	$(CC) -shared -o libmomentum.so momentum.o $(LIBS)

clean:
	rm -f *.so *.o
