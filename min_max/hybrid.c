/*
 * File: hybrid.c
 * Author: John Cieslewicz [johnc@cs.columbia.edu]
 * Copyright (c) 2007 The Trustees of Columbia University
 *
 * This code runs the sampling and aggregation with the hybrid method.
 * 
 */

#include "aggregate.h"
#include "global.h"

#include <atomic.h>
#include <thread.h>
#include <pthread.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <mtmalloc.h>

static inline void AddToGlobalAtomic(Aggregate a, const int id, 
			       uint64_t key, uint64_t min,
			       uint64_t max, uint64_t min2)
{
  register unsigned int i, index;
  register HashCell *current, *prev, *first;

  /* place oft used info in local variables */
  register const Tuple* input = a->input;
  register HashCell *buckets = a->global_buckets;
  register char *valid = a->valid;
						   

  register bool done = false; /* flag set when the current tuple is processed */
  index = mhash(key, a->lg_buckets);
      
  /* First check to see if the bucket has been visited before */
  if(!valid[index])
  {
    /* we're first, initialize the cell */
    MUTEX_LOCK(buckets[index].lock);
    
    /* recheck the bucket status after we aquire the lock */
    /* someone may have beat us here */	  
    if(valid[index] == 0)
      {
	buckets[index].key = key;
	buckets[index].min = min;
	buckets[index].max = max;
	buckets[index].min2 = min2;
	buckets[index].next = NULL;
	
	/* TODO: Because the valid bit is read unlocked above, */
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
      while(current!=NULL && current->key != key)
	{ 
	  prev = current;
	  current = current->next;
	}
      
      if(current)
	{	     
	  /* Found key -- update aggregate */	      
	  uint64_t cur, old;
	  old = current->min;	
	  while(min < old)
	    {
	      /* value is the new min */
	      old = atomic_cas_64(&(current->min), old, min);
	    }
	  
	  old = current->max;	
	  while(max > old)
	    {
	      /* value is the new max */
	      old = atomic_cas_64(&(current->max), old, max);
	    }
	  old = current->min2;	
	  while(min2 < old)
	    {
	      /* value is the new max */
	      old = atomic_cas_64(&(current->min2), old, min2);
	    }
/* 	  cur = current->min; */
/* 	  old = cur - 1; /\* so it does not match cur *\/ */
/* 	  while(min < cur && cur != old) */
/* 	    { */
/* 	      old = cur; */
/* 	      cur = atomic_cas_64(&(current->min), old, min); */
/* 	    } */

/* 	  cur = current->max; */
/* 	  old = cur - 1; /\* so it does not match cur *\/ */
/* 	  while(max > cur && cur != old) */
/* 	    { */
/* 	      old = cur; */
/* 	      cur = atomic_cas_64(&(current->max), old, max); */
/* 	    }	   */
/* 	  cur = current->min2; */
/* 	  old = cur - 1; /\* so it does not match cur *\/ */
/* 	  while(min2 < cur && cur != old) */
/* 	    { */
/* 	      old = cur; */
/* 	      cur = atomic_cas_64(&(current->min2), old, min2); */
/* 	    } */
	  done = true;	    
	}
      else
	{	      
	  /* Didn't find key, allocate new cell */
	  MUTEX_LOCK(buckets[index].lock);
	  if(buckets[index].next == first) 
	    {
	      /* as we did in earlier init code, make sure we weren't beaten */
	      current  = (HashCell*)malloc(sizeof(HashCell));
	      
	      current->key = key;
	      current->min = min;
	      current->max = max;
	      current->min2 = min2;
	      current->next = first;
	      //	      MUTEX_INIT(current->lock);
	      membar_exit();
	      /* Set last or other threads can see it before init!*/
	      /* TODO: As mentioned above, we may need a membar here */
	      buckets[index].next = current; 
	      done = true;
	    }
	  /* If we fail, we redo everything, instead of continuing where */
	  /* we left off...ok for now -- rarely happens */	      
	  MUTEX_UNLOCK(buckets[index].lock);
	}
    }  
}

void AggregateSample(Aggregate a, const int id, 
			    const int start, const int end, 
			    int *hits, int* num_runs)
{
  register unsigned int i, j, k, index;
  register uint64_t key, value;

  /* place oft used info in local variables */
  register const Tuple* input = a->input;
  register PrivateHashBucket *buckets = a->private_buckets[id];

  // do counting with local variables
  register int _hits, _num_runs;
  _hits = _num_runs = 0;
 
  for(i = start; i <= end; i++)
    {
      key = input[i].group;
      value = input[i].value;
      if(i > start && input[i-1].group != key)
	{
	  /* end of a run */
	  _num_runs ++;
	}

      index = mhash(key, a->lg_private_buckets);
      
      buckets[index].access_count++; // increment the count 
	  
      j = 0;
      while(j < PRIVATE_BUCKET_SIZE 
	    && buckets[index].valid[j] 
	    && buckets[index].data[j].key != key)
	j++;
      
      if(j < PRIVATE_BUCKET_SIZE)
	{
	  //FOUND key or empty slot
	  if(buckets[index].valid[j])
	    {
	      // Found key, do aggregation.
	      if(buckets[index].data[j].min > value)
		buckets[index].data[j].min = value;
	      if(buckets[index].data[j].max < value)
		buckets[index].data[j].max = value;
	      if(buckets[index].data[j].min2 > value)
		buckets[index].data[j].min2 = value;
	      _hits ++;
	    }
	  else
	    {
	      //Open slot, insert
	      buckets[index].data[j].key = key;
	      buckets[index].data[j].min = value;
	      buckets[index].data[j].max = value;
	      buckets[index].data[j].min2 = value;
	      buckets[index].valid[j] = 1;
	    }
	}
      else
	{
	  // Key not found. Need to evict.
	  // Push the last element into the global table
	  AddToGlobalAtomic(a, id, buckets[index].data[PRIVATE_BUCKET_SIZE - 1].key,
		      buckets[index].data[PRIVATE_BUCKET_SIZE - 1].min,
		      buckets[index].data[PRIVATE_BUCKET_SIZE - 1].max,
		      buckets[index].data[PRIVATE_BUCKET_SIZE - 1].min2);
	  
	  // Slide the existing values down, freeing up slot 0
	  for(k = PRIVATE_BUCKET_SIZE - 1; k >0; k --)
	    buckets[index].data[k] = buckets[index].data[k-1];
	  
	  // Put the new value in slot 0
	  buckets[index].data[0].key = key;
	  buckets[index].data[0].min = value;
	  buckets[index].data[0].max = value;
	  buckets[index].data[0].min2 = value;
	}    
    }
  //store hits and runs for return to caller
  *hits += _hits;
  *num_runs += _num_runs;
}

void AggregateHybrid(Aggregate a, const int id, 
			  const int start, const int end)
{
  register unsigned int i, j, k, index;
  register uint64_t key, value;

  /* place oft used info in local variables */
  register const Tuple* input = a->input;
  PrivateHashBucket *buckets = a->private_buckets[id];

  for(i = start; i <= end; i++)
    {
      key = input[i].group;
      value = input[i].value;
      j = 0;
      while(j < PRIVATE_BUCKET_SIZE 
	    && buckets[index].valid[j] 
	    && buckets[index].data[j].key != key)
	j++;

      if(j < PRIVATE_BUCKET_SIZE)
	{
	  //FOUND key or empty slot
	  if(buckets[index].valid[j])
	    {
	      // Found key, do aggregation.	  
	      if(buckets[index].data[j].min > value)
		buckets[index].data[j].min = value;
	      if(buckets[index].data[j].max < value)
		buckets[index].data[j].max = value;
	      if(buckets[index].data[j].min2 > value)
		buckets[index].data[j].min2 = value;
	    }
	  else
	    {
	      //Open slot, insert
	      buckets[index].data[j].key = key;
	      buckets[index].data[j].min = value;
	      buckets[index].data[j].max = value;
	      buckets[index].data[j].min2 = value;
	      buckets[index].valid[j] = 1;
	    }
	}
      else
	{
	  // Key not found. Need to evict.
	  // Push the last element into the global table
	  AddToGlobalAtomic(a, id, buckets[index].data[PRIVATE_BUCKET_SIZE - 1].key,
		      buckets[index].data[PRIVATE_BUCKET_SIZE - 1].min,
		      buckets[index].data[PRIVATE_BUCKET_SIZE - 1].max,
		      buckets[index].data[PRIVATE_BUCKET_SIZE - 1].min2);

	  // Slide the existing values down, freeing up slot 0
	  for(k = PRIVATE_BUCKET_SIZE - 1; k >0; k --)
	    buckets[index].data[k] = buckets[index].data[k-1];

	  // Put the new value in slot 0
	  buckets[index].data[0].key= key;
	  buckets[index].data[0].min = value;
	  buckets[index].data[0].max = value;
	  buckets[index].data[0].min2 = value;;
	}    
    }
}


// We put all the local data directly into the global table
void AggregateMergeLite(Aggregate a, const int id)
{
  int b, table, i;

  const int start_bucket = id * (a->n_private_buckets/a->n_threads);
  const int end_bucket = (id == a->n_threads-1) ? a->n_private_buckets : (id+1) *(a->n_private_buckets/a->n_threads);
  PrivateHashBucket *bucket;

  /* for all tables */
  for(table = 0; table < a->n_threads; table++)
    {
      /* for all buckets in my range */      
      for(b = start_bucket; b < end_bucket; b++)
	{
	  bucket = &(a->private_buckets[table][b]);
	  i = 0;
	  /* Do all the data elements in the current bucket */
	  while(i < PRIVATE_BUCKET_SIZE && bucket->valid[i])
	    {
	      AddToGlobalAtomic(a, id, 
			  bucket->data[i].key,
			  bucket->data[i].min,
			  bucket->data[i].max,
			  bucket->data[i].min2);
	      i++;
	    }	  	  
	}      
    }  
}
