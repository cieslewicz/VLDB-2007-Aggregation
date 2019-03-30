/*
 * File: main.c
 * Author: John Cieslewicz [johnc@cs.columbia.edu]
 * Copyright (c) 2007 The Trustees of Columbia University
 * 
 * Entry point for all experiments.
 */

#include "global.h"
#include "aggregate.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mtmalloc.h>
#include <assert.h>
#include <pthread.h>

//#include <libcpc.h>

#define NUM_RUNS 4

typedef struct InputInfo
{
  Tuple *tuples;
  int id;
  int numGroups;
  int numTups;
  int distribution;
  int power;
} InputInfo;

void *
fill_table (void *v)
{
  InputInfo *info;
  char buffer[256];
  int i, r;
  unsigned int chunksize, start, end;
  uint64_t temp;

  info = (InputInfo*)v;

  sprintf(buffer, "/local/johnc/niagra/input/INPUT_%d-%d-%d.%d.tup", info->power, info->numGroups, info->distribution, info->id);
  FILE *F = fopen(buffer, "rb");
  if(!F)
    {
      fprintf(stderr, "Could not open file: %s", buffer);
      exit(-1);
    }

  
  chunksize = info->numTups/MAX_THREADS;
  start = info->id * chunksize;
  end = (info->id == MAX_THREADS -1) ? info->numTups - 1 : (info->id+1)*chunksize - 1;
  //  printf("Starting read %d\n", info->id);
  for(i = start; i <= end; i++)
    {
      r = fread( &(info->tuples[i].group), sizeof(uint64_t), 1, F);
      //assert(r);
      r = fread( &(temp), sizeof(uint64_t), 1, F);      	
      //assert(r);
      //  printf("%lld\t%lld\n", info->tuples[i].group, info->tuples[i].value);
    }
  //printf("Ending read %d\n", info->id);
  fclose(F);
  return NULL;
}

void walk(void *arg, uint_t picno, const char *attr)
{
  printf("%s\n", attr);
}

int
main (int argc, char *argv[])
{
  unsigned int i, nGroups, nThreads, nTups, distribution, power, resample_rate;
  
  double exec_time, merge_time;
  Tuple *tuples;
  InputInfo info[MAX_THREADS];
  pthread_t threads[MAX_THREADS];
  Aggregate A;

  if (!(argc == 6))
    {
      fprintf(stderr, "Usage: %s <num tuples 2^k> <num groups> <num threads> <distribution code> <resample rate>\n", argv[0]);
      fprintf(stderr, "\tAvailable distributions:\n");
      fprintf(stderr, "\t\t0. Uniform\n");
      fprintf(stderr, "\t\t1. Sorted\n");
      fprintf(stderr, "\t\t2. 50%% Heavy Hitter\n");
      fprintf(stderr, "\t\t3. Repeated Sorted Runs\n");
      fprintf(stderr, "\t\t4. Zipf (theta = 0.5)\n");
      fprintf(stderr, "\t\t5. Self-similar (h = 0.2)\n");
      exit (-1);
    }

  // shift -- we express input as a power of 2
  power = atoi(argv[1]);
  nTups = 1 << power;
  nGroups = atoi (argv[2]);
  nThreads = atoi (argv[3]);
  distribution = atoi(argv[4]);
  resample_rate = atoi(argv[5]);

  assert (nTups > 0);
  assert (nGroups > 0);
  assert (nThreads >= 1);
  assert (distribution >= 0);
  assert (resample_rate >= 1);

  //  printf("Building Input\n");
  tuples = (Tuple*)malloc(sizeof(Tuple)*nTups);

  for (i = 0; i < MAX_THREADS; i++)
    {
      info[i].tuples = tuples;
      info[i].id = i;
      info[i].numGroups = nGroups;
      info[i].numTups = nTups;
      info[i].distribution = distribution;
      info[i].power = power;
      pthread_create (&threads[i], NULL, fill_table, &info[i]);
    }
  for (i = 0; i < MAX_THREADS; i++)
    pthread_join (threads[i], NULL);

  //throw away run 1
  A = AggregateCreate(nThreads, tuples, nTups, nGroups, resample_rate);
  exec_time = AggregateRun(A);
  merge_time = AggregateMerge(A);
  //  AggregateReset(A);
  
  exec_time = merge_time = 0.0;
  for(i = 0; i < NUM_RUNS; i++)
    {
      //      A = AggregateCreate(nThreads, tuples, nTups, nGroups, resample_rate);
      AggregateReset(A);
      exec_time += AggregateRun(A);
      merge_time += AggregateMerge(A);
  
    }

  exec_time = exec_time / NUM_RUNS;
  merge_time = merge_time / NUM_RUNS;

  printf("%d\t%d\t%d\t%f\t%f\t%f\t%f\t%f\t%f\t%d\n", 
	 nTups, 
	 nGroups, 
	 nThreads, 
	 exec_time,
	 exec_time * 1000000000.0 * nThreads / nTups, 
	 nTups / exec_time,
	 1.0-AggregateMissRate(A), /* hit rate */
	 AggregateMissRate(A), /* miss rate */
	 merge_time,
	 resample_rate
	 );
  
  //AggregatePrint(A);
}
