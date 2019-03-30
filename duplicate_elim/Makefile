#
# File: Makefile
# Author: John Cieslewicz [johnc@cs.columbia.edu]
# Copyright (c) 2007 The Trustees of Columbia University
#
# Simple make file to build the executables

TARGETS = aggregate_lock aggregate_atomic aggregate_partitioned aggregate_hybrid aggregate_adaptive aggregate_resample 
CC=cc

#FLAGS = -g -fast -xtarget=native64 -xdepend=yes -xunroll=8 -mt -lm 
#FLAGS = -g -fast -xtarget=native64 -xprefetch=explicit -mt -lm 


FLAGS = -g -fast -xtarget=native64 -mt -lm 
#FLAGS = -g -fast -D_PROFILE_ -xtarget=native64 -mt -lm 

LIBS= -lcpc -lpthread -lmtmalloc

.SUFFIXES: .c.o

all: $(TARGETS)

clean:
	rm -f *.o *~ $(TARGETS)

.c.o:
	$(CC) -c $(FLAGS) -o $@ $<

# executables

aggregate_lock: mutex.o aggregate_lock.o main.c
	$(CC) -o aggregate_lock  $(FLAGS) aggregate_lock.o mutex.o main.c $(LIBS)

aggregate_atomic: atomic.o aggregate_atomic.o mutex.o main.c
	$(CC) -o aggregate_atomic $(FLAGS) aggregate_atomic.o mutex.o atomic.o main.c $(LIBS)

aggregate_partitioned: aggregate_partitioned.o main.c
	$(CC) -o aggregate_partitioned $(FLAGS) aggregate_partitioned.o main.c $(LIBS)

aggregate_adaptive: aggregate_adaptive.o runs.o hybrid.o mutex.o atomic.o main.c 
	$(CC) -o aggregate_adaptive $(FLAGS) aggregate_adaptive.o runs.o hybrid.o atomic.o mutex.o main.c $(LIBS)

aggregate_resample: aggregate_resample.o runs.o hybrid.o mutex.o atomic.o main.c 
	$(CC) -o aggregate_resample $(FLAGS) aggregate_resample.o runs.o hybrid.o atomic.o mutex.o main.c $(LIBS)

aggregate_hybrid: aggregate_hybrid.o runs.o hybrid.o mutex.o atomic.o main.c 
	$(CC) -o aggregate_hybrid $(FLAGS) aggregate_hybrid.o runs.o hybrid.o atomic.o mutex.o main.c $(LIBS)
