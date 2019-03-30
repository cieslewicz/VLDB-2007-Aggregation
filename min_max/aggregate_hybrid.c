/*
 * File: aggregate_hybrid.c
 * Author: John Cieslewicz [johnc@cs.columbia.edu]
 * Copyright (c) 2007 The Trustees of Columbia University
 *
 * Using a fixed size local table, each thread spills from the local
 * to a shared, global table.
 */

#include "aggregate.h"
#include "global.h"
#include "timer.h"

#include <atomic.h>
#include <thread.h>
#include <pthread.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <mtmalloc.h>

/* Create a new aggregation object and return it to the caller */
Aggregate AggregateCreate(int n_threads, Tuple* tups, int n_tups, int n_groups, int resample_rate /* ignored */)
{
  register int i, j, k;

  Aggregate a;
  a = InitializeAggregate(n_threads, tups, n_tups, n_groups);

  InitializePrivateTables(a);

  return a;
}

/* static function that performs the aggregation for one thread */
static void AggregateOperate(Aggregate a, const int id)
{
  register unsigned int i;
  
  const unsigned int chunkSize = a->n_tups/a->n_threads;
  const unsigned int start = id * chunkSize;
  const unsigned int end = (id == a->n_threads-1) ? a->n_tups-1: chunkSize*(id+1)-1;

  AggregateHybrid(a, id, start, end);  
}

/* stub for thread to start in */
void * run_operate(void *v)
{
  ThreadInfo* info = (ThreadInfo*)v;
  AggregateOperate(info->a, info->id);
  return NULL;
}

/* stub for thread to start in for table merge */
void * run_merge(void *v)
{
  ThreadInfo* info = (ThreadInfo*)v;
  AggregateMergeLite(info->a, info->id);
  return NULL;
}

/* global entry point for running an aggregate */
/* creates threads that acutally do the aggregate, then collects them */
/* times aggregation */
double AggregateRun(Aggregate a)
{
  int i, r;
  double elapsed;
  Timer timer;
  pthread_t *threads;
  ThreadInfo *info;

  timer = TimerCreate();

  /* allocate space for the threads and their private data */
  threads = (pthread_t*)malloc(sizeof(pthread_t) * a->n_threads);
  info = (ThreadInfo*)malloc(sizeof(ThreadInfo) * a->n_threads);

  //  printf("Aggregating!\n");

  TimerStart(timer);

  /* set up thread info and start threads */  
  for(i = 0; i < a->n_threads; i++)
    {
      info[i].id = i;
      info[i].a = a;
      r = pthread_create(&threads[i], 
			 NULL, 
			 run_operate, 
			 &info[i]);
      assert(r==0);
    }

    /* join the theads */
  for(i = 0; i < a->n_threads; i++)
    pthread_join(threads[i], NULL);

  TimerStop(timer);
  elapsed = TimerElapsed(timer);

  /* clean up */
  TimerDelete(timer);
  free(threads);
  free(info);

  return elapsed;
}

double AggregateMerge(Aggregate a)
{
  int i, r;
  double elapsed;
  Timer timer;
  pthread_t *threads;
  ThreadInfo *info;

  timer = TimerCreate();

  /* allocate space for the threads and their private data */
  threads = (pthread_t*)malloc(sizeof(pthread_t) * a->n_threads);
  info = (ThreadInfo*)malloc(sizeof(ThreadInfo) * a->n_threads);


  TimerStart(timer);
  /* spawn threads to do the merge */
  for(i = 0; i < a->n_threads; i++)
    {
      info[i].id = i;
      info[i].a = a;
      r = pthread_create(&threads[i], 
			 NULL, 
			 run_merge, 
			 &info[i]);
      assert(r==0);
    }

  /* join threads */
  for(i = 0; i < a->n_threads; i++)
    pthread_join(threads[i], NULL);


  TimerStop(timer);
  elapsed = TimerElapsed(timer);

  /* clean up */
  TimerDelete(timer);
  free(threads);
  free(info);

  return elapsed;
}

/* Print out the contents of the valid hash table buckets */
void AggregatePrint(Aggregate a)
{
  HashCell *p;
  register int i, count;
  count = 0;
  for(i = 0; i < a->n_buckets; i++)
    {
      if(a->valid[i])
	{
	  p = &(a->global_buckets[i]);
	  /* process entire chain */
	  while(p!=NULL)
	    {
	      count ++;
	      printf("%d\t%d\t%lld\t%lld\t%lld\t%lld\n",
		     count,
		     i,
		     p->key,
		     p->min,
		     p->max,
		     p->min2);
	      p = p->next;
	    }
	}
    }
  //  printf("%d\n", count);
}

void AggregateReset(Aggregate a)
{
  ResetGlobalTable(a);
  ResetPrivateTables(a);
}
/* Clean up and free the table */
void AggregateDelete(Aggregate a)
{
  /* TODO -- free chained buckets */
  free(a->global_buckets);
  free(a);
}

double AggregateMissRate(Aggregate a)
{
  int hits = 0;
  for(int i = 0; i < a->n_threads; i++)
    hits += a->hits[i];
  return ( (double)(SAMPLE_SIZE * a->n_threads - hits) )/(SAMPLE_SIZE * a->n_threads);
}

