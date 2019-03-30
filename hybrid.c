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


void InitializePrivateTables(Aggregate a)
{
  register int i, j, k;

    /* Initialize Private Table Information */
  a->n_private_buckets = 1<<9;
  a->lg_private_buckets = log2(a->n_private_buckets);
  a->private_buckets = (PrivateHashBucket**)malloc(sizeof(PrivateHashBucket*) * a->n_threads);
  char* ptr = (char*)malloc( (sizeof(PrivateHashBucket)*a->n_private_buckets+8192+64) * a->n_threads); // we allocate extra to allow for setting the alignment.
  assert(ptr);

  /* TODO: If we are going to include initialization time in the */
  /*       running time, then this should be done in parallel */
  for(i=0; i < a->n_threads; i++)
    {      
      /* Ken noticed that there is an alignment issue, this fixes it */
      a->private_buckets[i] = (PrivateHashBucket*) ((unsigned long)(ptr + (i * (a->n_private_buckets * sizeof(PrivateHashBucket) + 8192 + 64) ) + i*(8192 / a->n_threads ) ) & (~63));
      
      //TODO compare with a bzero operation
      for(j = 0 ; j < a->n_private_buckets; j++)
	{
	  a->private_buckets[i][j].access_count = 0;
	  for(k = 0; k < PRIVATE_BUCKET_SIZE; k++)
	    a->private_buckets[i][j].valid[k] = 0;
	}
    }
}

void ResetPrivateTables(Aggregate a)
{
  int i,j,k;
  for(i = 0; i < a->n_threads; i++)
    for(j = 0; j < a->n_private_buckets; j++)
      {
	a->private_buckets[i][j].access_count = 0;
	for(k = 0; k < PRIVATE_BUCKET_SIZE; k++)
	  a->private_buckets[i][j].valid[k] = 0;
      }
}

static inline void AddToGlobalAtomic(Aggregate a, const int id, 
				     uint64_t key, 
				     uint64_t count1, uint64_t sum1, uint64_t square1,
				     uint64_t count2, uint64_t sum2, uint64_t square2,
				     uint64_t count3, uint64_t sum3, uint64_t square3,
				     uint64_t count4, uint64_t sum4
				     )
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

	buckets[index].sum1 = sum1;
	buckets[index].count1 = count1;
	buckets[index].squares1 = square1;

	buckets[index].sum2 = sum2;
	buckets[index].count2 = count2;
	buckets[index].squares2 = square2;

	buckets[index].sum3 = sum3;
	buckets[index].count3 = count3;
	buckets[index].squares3 = square3;

	buckets[index].sum4 = sum4;
	buckets[index].count4 = count4;

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

	  atomic_add_64(&(current->sum1),sum1); /* atomic add */
	  atomic_add_64(&(current->count1), count1); /* atomic increment */
	  atomic_add_64(&(current->square1), square1); /* atomic add */	  

	  atomic_add_64(&(current->sum2),sum2); /* atomic add */
	  atomic_add_64(&(current->count2), count2); /* atomic increment */
	  atomic_add_64(&(current->square2), square2); /* atomic add */	

	  atomic_add_64(&(current->sum3),sum3); /* atomic add */
	  atomic_add_64(&(current->count3), count3); /* atomic increment */
	  atomic_add_64(&(current->square3), square3); /* atomic add */	

	  atomic_add_64(&(current->sum4),sum4); /* atomic add */
	  atomic_add_64(&(current->count4), count4); /* atomic increment */

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

	      current->sum1 = sum1;
	      current->count1 = count1;
	      current->squares1 = square1;

	      current->sum2 = sum2;
	      current->count2 = count2;
	      current->squares2 = square2;

	      current->sum3 = sum3;
	      current->count3 = count3;
	      current->squares3 = square3;

	      current->sum4 = sum4;
	      current->count4 = count4;

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
  register uint64_t key;

  /* place oft used info in local variables */
  register const Tuple* input = a->input;
  register PrivateHashBucket *buckets = a->private_buckets[id];

  // do counting with local variables
  register int _hits, _num_runs;
  _hits = _num_runs = 0;
 
  for(i = start; i <= end; i++)
    {
      key = input[i].group;

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
		  buckets[index].data[j].count1 ++;
		  buckets[index].data[j].sum1 += input[i].value1;
		  buckets[index].data[j].squares1 += input[i].value1 * input[i].value1;

		  buckets[index].data[j].count2 ++;
		  buckets[index].data[j].sum2 += input[i].value2;
		  buckets[index].data[j].squares2 += input[i].value2 * input[i].value2;

		  buckets[index].data[j].count3 ++;
		  buckets[index].data[j].sum3 += input[i].value3;
		  buckets[index].data[j].squares3 += input[i].value3 * input[i].value3;

		  buckets[index].data[j].count4 ++;
		  buckets[index].data[j].sum4 += input[i].value4;

		  _hits ++;
	    }
	  else
	    {
	      //Open slot, insert
	      buckets[index].data[j].key = key;

	      buckets[index].data[j].count1 = 1;
	      buckets[index].data[j].sum1 = input[i].value1;
	      buckets[index].data[j].squares1 = input[i].value1 * input[i].value1;

	      buckets[index].data[j].count2 = 1;
	      buckets[index].data[j].sum2 = input[i].value2;
	      buckets[index].data[j].squares2 = input[i].value2 * input[i].value2;

	      buckets[index].data[j].count3 = 3;
	      buckets[index].data[j].sum3 = input[i].value3;
	      buckets[index].data[j].squares3 = input[i].value3 * input[i].value3;

	      buckets[index].data[j].count4 = 1;
	      buckets[index].data[j].sum4 = input[i].value4;

	      buckets[index].valid[j] = 1;
	    }
	}
      else
	{
	  // Key not found. Need to evict.
	  // Push the last element into the global table
	  AddToGlobalAtomic(a, id, buckets[index].data[PRIVATE_BUCKET_SIZE - 1].key,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].count1,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].sum1,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].squares1,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].count2,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].sum2,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].squares2,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].count3,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].sum3,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].squares3,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].count4,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].sum4
			    );
	  
	  // Slide the existing values down, freeing up slot 0
	  for(k = PRIVATE_BUCKET_SIZE - 1; k >0; k --)
	    buckets[index].data[k] = buckets[index].data[k-1];
	  
	  // Put the new value in slot 0
	  buckets[index].data[0].key = key;

	  buckets[index].data[0].count1 = 1;
	  buckets[index].data[0].sum1 = input[i].value1;
	  buckets[index].data[0].squares1 = input[i].value1 * input[i].value1;

	  buckets[index].data[0].count2 = 1;
	  buckets[index].data[0].sum2 = input[i].value2;
	  buckets[index].data[0].squares2 = input[i].value2 * input[i].value2;

	  buckets[index].data[0].count3 = 1;
	  buckets[index].data[0].sum3 = input[i].value3;
	  buckets[index].data[0].squares3 = input[i].value3 * input[i].value3;

	  buckets[index].data[0].count4 = 1;
	  buckets[index].data[0].sum4 = input[i].value4;
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
  register uint64_t key;

  /* place oft used info in local variables */
  register const Tuple* input = a->input;
  PrivateHashBucket *buckets = a->private_buckets[id];

  for(i = start; i <= end; i++)
    {
      key = input[i].group;
      index = mhash(key, a->lg_private_buckets);
      
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
	      buckets[index].data[j].count1 ++;
		  buckets[index].data[j].sum1 += input[i].value1;
		  buckets[index].data[j].squares1 += input[i].value1 * input[i].value1;

		  buckets[index].data[j].count2 ++;
		  buckets[index].data[j].sum2 += input[i].value2;
		  buckets[index].data[j].squares2 += input[i].value2 * input[i].value2;

		  buckets[index].data[j].count3 ++;
		  buckets[index].data[j].sum3 += input[i].value3;
		  buckets[index].data[j].squares3 += input[i].value3 * input[i].value3;

		  buckets[index].data[j].count4 ++;
		  buckets[index].data[j].sum4 += input[i].value4;
	    }
	  else
	    {
	      //Open slot, insert
	      buckets[index].data[j].key = key;

	      buckets[index].data[j].count1 = 1;
	      buckets[index].data[j].sum1 = input[i].value1;
	      buckets[index].data[j].squares1 = input[i].value1 * input[i].value1;

	      buckets[index].data[j].count2 = 1;
	      buckets[index].data[j].sum2 = input[i].value2;
	      buckets[index].data[j].squares2 = input[i].value2 * input[i].value2;

	      buckets[index].data[j].count3 = 3;
	      buckets[index].data[j].sum3 = input[i].value3;
	      buckets[index].data[j].squares3 = input[i].value3 * input[i].value3;

	      buckets[index].data[j].count4 = 1;
	      buckets[index].data[j].sum4 = input[i].value4;

	      buckets[index].valid[j] = 1;
	    }
	}
      else
	{
	  // Key not found. Need to evict.
	  // Push the last element into the global table
	  AddToGlobalAtomic(a, id, buckets[index].data[PRIVATE_BUCKET_SIZE - 1].key,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].count1,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].sum1,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].squares1,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].count2,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].sum2,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].squares2,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].count3,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].sum3,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].squares3,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].count4,
			    buckets[index].data[PRIVATE_BUCKET_SIZE - 1].sum4
			    );
	  
	  // Slide the existing values down, freeing up slot 0
	  for(k = PRIVATE_BUCKET_SIZE - 1; k >0; k --)
	    buckets[index].data[k] = buckets[index].data[k-1];

	  // Put the new value in slot 0
	  buckets[index].data[0].key = key;

	  buckets[index].data[0].count1 = 1;
	  buckets[index].data[0].sum1 = input[i].value1;
	  buckets[index].data[0].squares1 = input[i].value1 * input[i].value1;

	  buckets[index].data[0].count2 = 1;
	  buckets[index].data[0].sum2 = input[i].value2;
	  buckets[index].data[0].squares2 = input[i].value2 * input[i].value2;

	  buckets[index].data[0].count3 = 1;
	  buckets[index].data[0].sum3 = input[i].value3;
	  buckets[index].data[0].squares3 = input[i].value3 * input[i].value3;

	  buckets[index].data[0].count4 = 1;
	  buckets[index].data[0].sum4 = input[i].value4;
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
				bucket->data[i].count1,
				bucket->data[i].sum1,
				bucket->data[i].squares1,
				bucket->data[i].count2,
				bucket->data[i].sum2,
				bucket->data[i].squares2,
				bucket->data[i].count3,
				bucket->data[i].sum3,
				bucket->data[i].squares3,
				bucket->data[i].count4,
				bucket->data[i].sum4
				);
	      i++;
	    }	  	  
	}      
    }  
}
