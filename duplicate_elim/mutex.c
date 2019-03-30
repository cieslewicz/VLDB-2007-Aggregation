/*
 * File: mutex.c
 * Author: John Cieslewicz [johnc@cs.columbia.edu]
 * Copyright (c) 2007 The Trustees of Columbia University
 *
 * This file contains code to do global aggregation using mutexes
 * The global table initialization functions are also included.
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
#include <strings.h>

void * run_init(void *v)
{
  register int i;
  ThreadInfo *info = (ThreadInfo*)v;
  Aggregate a = info->a;

  const unsigned int chunkSize = a->n_buckets/a->n_threads;
  const unsigned int start = info->id * chunkSize;
  const unsigned int end = (info->id == a->n_threads-1) ? a->n_buckets-1: chunkSize*(info->id+1)-1;

  HashCell *end_bucket = &(a->global_buckets[end]);
  HashCell *current_bucket = &(a->global_buckets[start]);
  char *current_valid = &(a->valid[start]);

  for(; current_bucket <= end_bucket; current_bucket++, current_valid++)
    {
      MUTEX_INIT(current_bucket->lock);
      (*current_valid) = 0;
    }

  /*   for(i = start ; i <= end ; i++) */
  /*     { */
  /*       MUTEX_INIT(a->global_buckets[i].lock); */
  /*       a->valid[i] = 0; */
  /*     } */
  
  return NULL;
}

Aggregate InitializeAggregate(int n_threads, 
			      Tuple *tups, 
			      int n_tups, 
			      int n_groups)
{
  register int i;
  char * ptr;
  ThreadInfo *info;
  Aggregate a;

  assert(n_threads > 0);

  a = (Aggregate)malloc(sizeof(AggregateCDT));
  a->n_threads = n_threads;
  a->n_tups = n_tups;
  a->input = tups;

  // We assume that the number of groups is a power of 2
  a->n_buckets = (n_groups < 32) ? 32 : n_groups * 2;
  //a->n_buckets = 1 << 17;

  ptr = (char*)malloc(sizeof(HashCell) * a->n_buckets + 64); //align to 64 byte cache line
  a->global_buckets = (HashCell*)( (unsigned long)(ptr + 64) & (~63) );
  assert(a->global_buckets);

  a->valid = (char*)malloc(sizeof(char) * a->n_buckets);
  assert(a->valid);

  a->lg_buckets = log2(a->n_buckets);

  // WARM UP
/*    int temp = 0;  */
/*    for(i = 0; i < a->n_buckets; i ++)  */
/*      {	  */
/*        temp += *(int*)(&a->global_buckets[i]);  */
/*        temp += a->valid[i];  */
/*      }  */
/*    int X[1000000]; */
/*    for(i = 0 ; i < 1000000; i++) */
/*      temp += X[i]; */
/*    a->hits[0] = temp; // store so compiler won't rid it */

  /* Initialize the table */
  /* TODO: If we are going to include initialization time in the */
  /*       running time, then this should be done in parallel */
  if(a->n_buckets < 10000)
    {
      /* Serial Initialization */
      for(i = 0; i < a->n_buckets; i++)
	{
	  MUTEX_INIT(a->global_buckets[i].lock);
	  a->valid[i] = 0;
	} 
    }
  else
    {
      ThreadInfo *info = (ThreadInfo*)malloc(sizeof(ThreadInfo)*a->n_threads);
      pthread_t *threads = (pthread_t*)malloc(sizeof(pthread_t)*a->n_threads);
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

void DeleteGlobalTable(Aggregate a)
{
  for(int i = 0; i < a->n_buckets; i++)
    {
      if(a->valid[i])
	{
	  HashCell *prev, *current;
	  prev = NULL;
	  current = a->global_buckets[i].next;
	  while(current != NULL)
	    {
	      if(prev != NULL)
		free(prev);
	      prev = current;
	      current = current->next;
	    }
	  if(prev != NULL)
	    free(prev);
	}
    }
  free(a->global_buckets);  
  a->global_buckets = NULL;
  free(a->valid);
  a->valid = NULL;
}

void ResetGlobalTable(Aggregate a)
{
  for(int i = 0; i < a->n_buckets; i++)
    {
      if(a->valid[i])
	{
	  HashCell *prev, *current;
	  prev = NULL;
	  current = a->global_buckets[i].next;
	  while(current != NULL)
	    {
	      if(prev != NULL)
		free(prev);
	      prev = current;
	      current = current->next;
	    }
	  if(prev != NULL)
	    free(prev);
	}
      a->valid[i] = 0;
      a->global_buckets[i].next = NULL;
    }
}

/* Insert into the global table */
void AggregateMutex(Aggregate a, const int id, 
			    const int start, const int end)
{
  register unsigned int i, index;
  HashCell *current, *prev, *first;
  
  /* place oft used info in local variables */
  const unsigned int lg_buckets = a->lg_buckets;
  const Tuple* input = a->input;
  register HashCell *buckets = a->global_buckets;
  register char* valid = a->valid;

  for(i = start; i<=end; i++)
    {
      bool done = false; /* flag set when the current tuple is processed */
      //hash = joaat_hash_hardcoded((unsigned char*)&(input[i].group));
      //index = hash & a->BUCKET_MASK;
      index = mhash(input[i].group, lg_buckets);
      
      /* First check to see if the bucket has been visited before */
      if(!valid[index])
	{
	  /* we're first, initialize the cell */
	  MUTEX_LOCK(buckets[index].lock);

	  /* recheck the bucket status after we aquire the lock */
	  /* someone may have beat us here */	  
	  if(valid[index] == 0)
	    {
	      buckets[index].key = input[i].group;
	      buckets[index].next = NULL;

	      /*TODO: Because the valid bit is read unlocked above, */
	      /* we may need a membar_exit before setting valid to */
	      /* ensure that the previous stores are globally visible */
	      /* before the valid bit is */
	      membar_exit();
	      valid[index] = 1; /*set last or immediatley valid...*/
	      done = true;
	    }	  
	  MUTEX_UNLOCK(buckets[index].lock);
	}
      
      /* if !done we didn't initialize a cell above */
      while(!done)
	{
	  /* the bucket is valid, so look at the chain*/	  
	  first = buckets[index].next;
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
	      /* found a duplicate */
	      done = true;	    
	    }
	  else
	    {	      
	      /* Didn't find key, allocate new cell at beginning */
	      MUTEX_LOCK(buckets[index].lock);
	      /* as we did in earlier init code, make sure we weren't beaten */
	      if(buckets[index].next == first) 
		{
		  current  = (HashCell*)malloc(sizeof(HashCell));		  
		  current->key = input[i].group;
		  current->next = first;
		  MUTEX_INIT(current->lock);
		  /* Set last or other threads can see it before init!*/
		  /* TODO: As mentioned above, we may need a membar here */
		  membar_exit();
		  buckets[index].next = current; 
		  done = true;
		}
	      /* If we fail, we redo everything, instead of continuing where */
	      /* we left off...ok for now -- rarely happens */	      
	      MUTEX_UNLOCK(buckets[index].lock);
	    }
	}
    }    
}
