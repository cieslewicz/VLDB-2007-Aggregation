/*
 * File: aggregate_indep.c
 * Author: John Cieslewicz [johnc@cs.columbia.edu]
 * Copyright (c) 2007 The Trustees of Columbia University
 *
 * Aggregation using a private table for each thread.
 */

#include "aggregate.h"
#include "global.h"
#include "timer.h"

#include <thread.h>
#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <mtmalloc.h>
#include <math.h>

void * run_init(void *v)
{
  register int i;
  ThreadInfo *info = (ThreadInfo*)v;
  Aggregate a = info->a;
  const int id = info->id;

  for(i = 0 ; i < a->n_buckets; i++)
    {
      a->independent_cells[id][i].valid = 0;
      a->independent_cells[id][i].next = NULL;
    }

  return NULL;
}

/* Create a new aggregation object and return it to the caller */
Aggregate AggregateCreate(int n_threads, Tuple *tups, int n_tups, int n_groups, int resample_rate /* ignored */)
{
  register int i, j;
  Aggregate a;
  assert(n_threads > 0);

  a = (Aggregate)malloc(sizeof(AggregateCDT));
  a->n_threads = n_threads;
  a->n_tups = n_tups;
  a->input = tups;

  // We assume that the number of groups is a power of 2
  //a->n_buckets = 1 << 17;
  a->n_buckets = (n_groups < 32) ? 32 : n_groups * 2;
  a->lg_buckets = log2(a->n_buckets);

  /* allocate the pointers to the hash tables */
  a->independent_cells = (IndependentHashCell**)malloc(sizeof(IndependentHashCell*) * a->n_threads);
  assert(a->independent_cells);
  char* ptr = (char*)malloc( (sizeof(IndependentHashCell)*a->n_buckets+8192+64) * a->n_threads); // we allocate extra to allow for setting the alignment.
  assert(ptr);  

  /* Initialize the table */
  /* TODO: If we are going to include initialization time in the */
  /*       running time, then this should be done in parallel */
  for(i = 0; i < n_threads; i++)
    {
      /* Ken noticed that there is an alignment issue, this fixes it */
      // align to the 64 byte L2 cache, choose a different offset in the 8K L1.
      a->independent_cells[i] = (IndependentHashCell*) ((unsigned long)(ptr + (i * (a->n_buckets * sizeof(IndependentHashCell) + 8192 + 64) ) + i*(8192 / n_threads ) ) & (~63));
      
    }

  if(a->n_buckets < 1000)
    {
      /* do serially */
       for(i = 0; i < n_threads; i++)
	 {
	   
	   for(j = 0 ; j < a->n_buckets; j++)
	     {
	       a->independent_cells[i][j].valid = 0;
	       a->independent_cells[i][j].next = NULL;
	     }
	 }
    }
  else
    {
      /* do with all threads */
      ThreadInfo *info = (ThreadInfo*)malloc(sizeof(ThreadInfo) * a->n_buckets);
      pthread_t *threads = (pthread_t*)malloc(sizeof(pthread_t) * a->n_buckets);
      assert(info && threads);
      
      for(i = 0; i < a->n_threads; i++)
	{
	  info[i].id = i;
	  info[i].a = a;
	  pthread_create(&threads[i], NULL, run_init, &info[i]);
	}
      
      for(i = 0; i < a->n_threads; i++)
	pthread_join(threads[i], NULL);
      free(info);
      free(threads);
    }

  return a;
}

/* static function that performs the aggregation for one thread */
static void AggregateOperate(Aggregate a, const int id)
{
  register unsigned int i, index;
  register IndependentHashCell *current, *prev;

  /* place oft used info in local variables */
  register const int lg_buckets = a->lg_buckets;
  register const Tuple* input = a->input;
  register IndependentHashCell *buckets = a->independent_cells[id];

  const unsigned int chunkSize = a->n_tups/a->n_threads;
  register const unsigned int start = id * chunkSize;
  register const unsigned int end = (id == a->n_threads-1) ? a->n_tups-1: chunkSize*(id+1)-1;

  for(i = start; i <= end; i++)
    {
      index = mhash(input[i].group, lg_buckets);
      if(buckets[index].valid == 0)
	{
	  /* unused slot, add our info and we're done */
	  buckets[index].key = input[i].group;
	  buckets[index].min = input[i].value;
	  buckets[index].max = input[i].value;
	  buckets[index].min2 = input[i].value;
	  buckets[index].next = NULL;
	  buckets[index].valid = 1;
	}	   
      else
	{
	  /* the bucket is valid, so look at the chain*/
	  current = &buckets[index];
	  prev = NULL;

	  /* is key already there? */
	  while(current!=NULL && current->key != input[i].group)
	    {
	      prev = current;
	      current = current->next;	      
	    }
	  
	  if(current)
	    {	   
	      /* Found key -- update aggregate */
	      if(current->min > input[i].value)
		current->min = input[i].value;
	      if(current->max < input[i].value)
		current->max = input[i].value;
	      if(current->min2 > input[i].value)
		current->min2 = input[i].value;	      
	    }
	  else
	    {	 
	      /* Didn't find key, allocate new cell */
	      current  = (IndependentHashCell*)malloc(sizeof(IndependentHashCell));
	      assert(current);
	      current->key = input[i].group;
	      current->min = input[i].value;
	      current->max = input[i].value;
	      current->min2 = input[i].value;
	      //add to front
	      current->next = buckets[index].next;	      
	      buckets[index].next = current;
	    } 
	}    
    }    
}

/* append or update the contents of p into d and its chain */
static void update_or_append(IndependentHashCell *d, IndependentHashCell *p)
{
  IndependentHashCell *prev;
  if(!d->valid)
    {
      /* the bucket is open, so add our info here*/
      d->valid = 1;
      d->key = p->key;
      d->min = p->min;
      d->max = p->max;
      d->min2 = p->min2;
      d->next = NULL;
    }
  else
    {
      /* look for the key in this chain */
      prev = NULL;
      while( d!= NULL && d->key != p->key)
	{
	  prev = d;
	  d = d->next;
	}

      if(d == NULL)
	{
	  /* key did not exist, add to chain */
	  d = (IndependentHashCell*)malloc(sizeof(IndependentHashCell));
	  assert(d);
	  d->key = p->key;
	  d->min = p->min;
	  d->max = p->max;
	  d->min2 = p->min2;
	  d->next = NULL;
	  prev->next = d;
	}
      else
	{
	  /* found it, so add our information */
	  assert(d->key == p->key);
	  if(d->min > p->min)
	    d->min = p->min;
	  if(d->max > p->max)
	    d->max = p->max;
	  if(d->min2 > p->min2)
	    d->min2 = p->min2;	  
	}
    }
}

/* put all the independent table aggregates into table 0 */
static void Merge(Aggregate a, const int id)
{
  int bucket, table;
  IndependentHashCell *p;

  /* decide what range of buckets to process based on ID */
  const int start_bucket = id * (a->n_buckets/a->n_threads);
  const int end_bucket = (id == a->n_threads-1) ? a->n_buckets : (id+1) *(a->n_buckets/a->n_threads);

  /* for all buckets in my range */
  for(bucket = start_bucket; bucket < end_bucket; bucket++)
    {
      /* for all tables */
      for(table = 1; table < a->n_threads; table++)
	{
	  /* if valid, do bucket and chain */
	  if(a->independent_cells[table][bucket].valid == 1)
	    {
	      p = &(a->independent_cells[table][bucket]);
	      while(p!=NULL)
		{
		  update_or_append(&(a->independent_cells[0][bucket]), p);
		  p = p->next;
		}
	    }
	}      
    }
}

/* stub for thread to start in for aggregation */
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
  Merge(info->a, info->id);
  return NULL;
}

double AggregateRun(Aggregate a)
{
  int i, r;
  double elapsed;
  Timer timer;
  pthread_t *threads;
  ThreadInfo *info;

  /* allocate space for the threads and their private data */
  threads = (pthread_t*)malloc(sizeof(pthread_t) * a->n_threads);
  info = (ThreadInfo*)malloc(sizeof(ThreadInfo) * a->n_threads);

  timer = TimerCreate();

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

double AggregateMerge(Aggregate a)
{
  int i, r;
  double elapsed;
  Timer timer;
  pthread_t *threads;
  ThreadInfo *info;

  /* allocate space for the threads and their private data */
  threads = (pthread_t*)malloc(sizeof(pthread_t) * a->n_threads);
  info = (ThreadInfo*)malloc(sizeof(ThreadInfo) * a->n_threads);

  timer = TimerCreate();

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
  int i, count;
  IndependentHashCell *p;

  count = 0;
  for(i = 0; i < a->n_buckets; i++)
    {
      if(a->independent_cells[0][i].valid)
	{
	  IndependentHashCell *p = &(a->independent_cells[0][i]);
	  while(p != NULL)
	    {
	      count ++;
	      printf("%d\t%d\t%lld\t%lld\t%lld\t%lld\n", 
		     count, 
		     i, 
		     p->key,
		     p->min,
		     p->max,
		     p->min2
		     );
	      p = p->next;
	    }
	}
    }
}

void AggregateReset(Aggregate a)
{
  //TODO Delete memory...
  for(int i = 0; i < a->n_threads; i++)
    for(int j = 0; j < a->n_buckets; j++)
    {
      if(a->independent_cells[i][j].valid)
	{
	  IndependentHashCell *cur, *prev;
	  prev = NULL;
	  cur = a->independent_cells[i][j].next;
	  while(cur != NULL)
	    {
	      if(prev != NULL)
		free(prev);
	      prev = cur;
	      cur = cur->next;
	    }
	  if(prev != NULL)
	    free(prev);
	}

      a->independent_cells[i][j].valid = 0;
      a->independent_cells[i][j].next = NULL;
    }
}

/* Clean up and free the table */
void AggregateDelete(Aggregate a)
{
  int i;
  //TODO - scan buckets, if valid, look at next (may have to delete the chain)
  for(i = 0; i < a->n_threads; i++)
    free( a->independent_cells[i]);
  free(a->independent_cells);
  free(a);
}
double AggregateMissRate(Aggregate a)
{
  return 0.0;
}

