/*
 * File: aggregate_atomic.c
 * Author: John Cieslewicz [johnc@cs.columbia.edu]
 * Copyright (c) 2007 The Trustees of Columbia University
 *
 * This version of multi-core aggregation uses a mutex to
 * protect every hash cell during initialization and chaining.
 * An atomic update is performed on the aggregation value. 
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
  return InitializeAggregate(n_threads, tups, n_tups, n_groups);
}

/* static function that performs the aggregation for one thread */
static void AggregateOperate(Aggregate a, const int id)
{
  const unsigned int chunkSize = a->n_tups/a->n_threads;
  const unsigned int start = id * chunkSize;
  const unsigned int end = (id == a->n_threads-1) ? a->n_tups-1: chunkSize*(id+1)-1;
  
  AggregateAtomic(a, id, start, end);
}

/* stub for thread to start in */
void * run_operate(void *v)
{
  ThreadInfo* info = (ThreadInfo*)v;
  AggregateOperate(info->a, info->id);

  return NULL;
}

/* global entry point for running an aggregate */
/* creates threads that acutally do the aggregate, then collects them */
/* times aggregation */
double AggregateRun(Aggregate a)
{
  int i, r;
  double elapsed;
  Timer t;
  pthread_t *threads;
  ThreadInfo *info;

  t = TimerCreate();

  /* allocate space for the threads and their private data */
  threads = (pthread_t*)malloc(sizeof(pthread_t) * a->n_threads);
  info = (ThreadInfo*)malloc(sizeof(ThreadInfo) * a->n_threads);

  TimerStart(t);

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


  TimerStop(t);
  elapsed = TimerElapsed(t);

  /* clean up */
  TimerDelete(t);
  free(threads);
  free(info);

  return elapsed;
}

double AggregateMerge(Aggregate a)
{
  return 0.0;
}

/* Print out the contents of the valid hash table bucets */
void AggregatePrint(Aggregate a)
{
  HashCell *p;
  int i, count;
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
		     p->count1,
		     p->sum1,
		     p->squares1
		     );
	      p = p->next;
	    }
	}
    }
}

/* Clean up and free the table */
void AggregateDelete(Aggregate a)
{
  /* TODO -- free chained buckets */
  DeleteGlobalTable(a);
  free(a);
}

void AggregateReset(Aggregate a)
{
  ResetGlobalTable(a);
}

double AggregateMissRate(Aggregate a)
{
  return 0.0;
}

