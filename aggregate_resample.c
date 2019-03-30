/*
 *
 * File: aggregate_resample.c
 * Author: John Cieslewicz [johnc@cs.columbia.edu]
 * Copyright (c) 2007 The Trustees of Columbia University
 *
 * Using a fixed size local table, each thread engages in sampling to 
 * determine an estimate of the distribution. The sampling is repeated
 * k times to adjust to changing distributions.
 *
 * We sample for:
 * (1) Access counts to buckets
 * (2) Hits in the table (i.e. items already in the table)
 * (3) Runs of same group-by key in consecutive tuples
 * From these statistics we determine: miss rate, contention, avg. run length
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
Aggregate AggregateCreate(int n_threads, Tuple* tups, int n_tups, int n_groups, int resample_rate)
{
  register int i, j, k;

  Aggregate a;
  
  a = InitializeAggregate(n_threads, tups, n_tups, n_groups);

  assert(resample_rate >= 1);
  a->resample_rate = resample_rate;

  InitializePrivateTables(a);

  return a;
}

static inline void ResetLocalTable(Aggregate a, const int id)
{
  register int i;
  for(i = 0; i < a->n_private_buckets; i++)
    a->private_buckets[id][i].access_count = 0;
}

/* static function that performs the aggregation for one thread */
static void AggregateOperate(Aggregate a, const int id)
{
  register unsigned int i, j, k;
  unsigned int my_partition;
  
  while( (my_partition= atomic_inc_uint_nv(&(a->current_partition))) <= a->n_partitions)
    {
      // the current partition starts at 0, increment returns new value, 
      //so the values returned count [1, n_partitions], but we want [0, n_partitions)
      my_partition = my_partition - 1; 

      ResetLocalTable(a, id); //should be quick...
      
      int hits = 0;
      int num_runs = 1;
      
      const unsigned int start = my_partition * (double)a->n_tups/a->n_partitions;
      const unsigned int end = (my_partition == a->n_partitions-1) ? a->n_tups-1: (my_partition+1)*(double)a->n_tups/a->n_partitions - 1;
      const int warmup_end = start + WARMUP; 
      const int sample_end = warmup_end + SAMPLE_SIZE;

      //printf("[%d]\t%d\t%d\t%d\t%d\n", id, my_partition, end - start, start, end);
      
      assert(warmup_end < end && sample_end < end);

      AggregateSample(a, id, 
		      start, warmup_end-1, 
		      &hits, &num_runs);
      
      hits = 0;
      AggregateSample(a, id, 
		      warmup_end, sample_end - 1, 
		      &hits, &num_runs);
      
      /* calculate the max accesses */
      int max[7];
      for(i = 0; i < 7; i++)
	max[i] = 0; //init to 0
      for(i = 0; i < a->n_private_buckets; i++)    
	for(j = 0; j < 7; j++)       
	  /* check all maxes */
	  if(max[j] < a->private_buckets[id][i].access_count)
	    {
	      /* found this value's spot */
	      /* slide all others down */
	      for( k = 7-1; k > j; k--)
		max[k] = max[k-1];
	      max[j] = a->private_buckets[id][i].access_count;
	    }
      
      double estimate_sum = 0.0;
      double f;
      for(i = 0; i < 7; i++)
	{
	  f = (double)max[i]/( SAMPLE_SIZE + WARMUP);
	  if( f >= 1.0/7.58)
	    {
	      estimate_sum += 25.1 *f - 3.31;
	    }
	  else
	    {
	      break; //no subsequent max will meet the threshold, either.
	    }
	}
      
      double avg_run_length = (double)(SAMPLE_SIZE + WARMUP) / num_runs;
      double missrate = ( (double)( SAMPLE_SIZE - hits) )/(SAMPLE_SIZE);
      
      if(avg_run_length > 1.142857)
	{
	  /* Runs are present */
	  AggregateRuns(a, id, sample_end, end);
	}
  //  else if(missrate < 0.5 || max > (SAMPLE_SIZE + WARMUP)/16)
      else if (missrate < 0.5 || estimate_sum >= 1.0)
	{
	  /* locallity or contention */
	  AggregateHybrid(a, id, sample_end, end);
	}
      else
	{
	  /* no locallity or contention, use global table */
	  AggregateAtomic(a, id, sample_end, end);
	}
      
      a->hits[id] = hits;
    }
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

  a->n_partitions = a->n_threads * a->resample_rate;
  a->current_partition = 0;
  timer = TimerCreate();

  /* allocate space for the threads and their private data */
  threads = (pthread_t*)malloc(sizeof(pthread_t) * a->n_threads);
  info = (ThreadInfo*)malloc(sizeof(ThreadInfo) * a->n_threads);

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
  
    /* respawn threads to do the merge */
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
		     p->count1,
		     p->sum1,
		     p->squares1);
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
