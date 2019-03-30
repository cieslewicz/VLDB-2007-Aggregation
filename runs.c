/*
 * File: runs.c
 * Author: John Cieslewicz [johnc@cs.columbia.edu]
 * Copyright (c) 2007 The Trustees of Columbia University
 *
 * This file implements the run based optimization on the hybrid
 * and global table.
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

/*
 * Aggregate with run optimization, but push directly to the global table.
 */
void AggregateRunsGlobal(Aggregate a, const int id,
			 const int start, const int end)
{ 
  
  register unsigned int i, j, k, index;
  register uint64_t key;
  uint64_t sum1, square1, count1;
  uint64_t sum2, square2, count2;
  uint64_t sum3, square3, count3;
  uint64_t sum4, square4;
  register HashCell *current, *prev, *first;
  /* place oft used info in local variables */  
  register const Tuple* input = a->input;
  
  register HashCell *buckets = a->global_buckets;
  register char *valid = a->valid;

  key = input[start].group;

  sum1 = input[start].value1;
  square1 = input[start].value1 * input[start].value1;
  
  sum2 = input[start].value2;
  square2 = input[start].value2 * input[start].value2;

  sum3 = input[start].value3;
  square3 = input[start].value3 * input[start].value3;

  sum4 = input[start].value4;
  square4 = input[start].value4 * input[start].value4;

  count1 = count2 = count3 = count4 = 1;


  for(i = start+1; i <= end; i++)
    {
      if(key == input[i].group)
	{
	  /* this is a run */
	  sum1 += input[i].value1;
	  square1 += input[i].value1 * input[i].value1;
	  count1 ++;

	  sum2 += input[i].value2;
	  square2 += input[i].value2 * input[i].value2;
	  count2 ++;	  

	  sum3 += input[i].value3;
	  square3 += input[i].value3 * input[i].value3;
	  count3 ++;

	  sum4 += input[i].value4;
	  square4 += input[i].value4 * input[i].value4;

	}
      else
	{	
	  register bool done = false; /* flag set when the current tuple is processed */
	  //  hash = joaat_hash_hardcoded((unsigned char*)&key);
	  //  index = hash & a->BUCKET_MASK;
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
		  atomic_add_64(&(current->squares1), square1); /* atomic add */	  

		  atomic_add_64(&(current->sum2),sum2); /* atomic add */
		  atomic_add_64(&(current->count2), count2); /* atomic increment */
		  atomic_add_64(&(current->squares2), square2); /* atomic add */

		  atomic_add_64(&(current->sum3),sum3); /* atomic add */
		  atomic_add_64(&(current->count3), count3); /* atomic increment */
		  atomic_add_64(&(current->squares3), square3); /* atomic add */

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
	  
	  /* The current tuple is the start of a run */
	  key = input[start].group;
	  
	  sum1 = input[start].value1;
	  square1 = input[start].value1 * input[start].value1;
	  
	  sum2 = input[start].value2;
	  square2 = input[start].value2 * input[start].value2;
	  
	  sum3 = input[start].value3;
	  square3 = input[start].value3 * input[start].value3;
	  
	  sum4 = input[start].value4;
	  square4 = input[start].value4 * input[start].value4;
	  
	  count1 = count2 = count3 = count4 = 1;
	}
    }

  /* flush the last tuple/run */
  AddToGlobalAtomic(a, id, key, 
		    count1, sum1, square1,
		    count2, sum2, square2,
		    count3, sum3, square3,
		    count4, sum4
		    ); 
}

/*
 * Aggregate with run optimization, push tuples into the local table.
 */
void AggregateRuns(Aggregate a, const int id, 
		   const int start, const int end)
{
  register unsigned int i, j, k, hash, index;
  register uint64_t key;
  uint64_t sum1, square1, count1;
  uint64_t sum2, square2, count2;
  uint64_t sum3, square3, count3;
  uint64_t sum4, square4;
  /* place oft used info in local variables */  
  register const Tuple* input = a->input;
  
  //PrivateHashCell **buckets = a->private_buckets;
  //TODO - could we be more specific about what we store here and do
  // PrivateHashCell *buckets = a->private_buckets[id]] ???
  register PrivateHashBucket *buckets = a->private_buckets[id];
 
  key = input[start].group;
  
  sum1 = input[start].value1;
  square1 = input[start].value1 * input[start].value1;
  
  sum2 = input[start].value2;
  square2 = input[start].value2 * input[start].value2;
  
  sum3 = input[start].value3;
  square3 = input[start].value3 * input[start].value3;
  
  sum4 = input[start].value4;
  square4 = input[start].value4 * input[start].value4;
  
  count1 = count2 = count3 = count4 = 1;

  for(i = start+1; i <= end; i++)
    {
      if(key == input[i].group)
	{ 
	  /* this is a run */
	  sum1 += input[i].value1;
	  square1 += input[i].value1 * input[i].value1;
	  count1 ++;

	  sum2 += input[i].value2;
	  square2 += input[i].value2 * input[i].value2;
	  count2 ++;	  

	  sum3 += input[i].value3;
	  square3 += input[i].value3 * input[i].value3;
	  count3 ++;

	  sum4 += input[i].value4;
	  square4 += input[i].value4 * input[i].value4;

	}
      else
	{	
	  /* Run ended, start next run */

	  //hash = joaat_hash_hardcoded((unsigned char*)&key);
	  //index = hash & MASK;      
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
		  buckets[index].data[j].count1 += count1;
		  buckets[index].data[j].sum1 += sum1;
		  buckets[index].data[j].squares1 += square1;

		  buckets[index].data[j].count2 += count2;
		  buckets[index].data[j].sum2 += sum2;
		  buckets[index].data[j].squares2 += square2;

		  buckets[index].data[j].count3 += count3;
		  buckets[index].data[j].sum3 += sum3;
		  buckets[index].data[j].squares3 += square3;

		  buckets[index].data[j].count4 += count4;
		  buckets[index].data[j].sum4 += sum4;
		}
	      else
		{
		  //Open slot, insert
		  buckets[index].data[j].key = key;

		  buckets[index].data[j].count1 = count1;
		  buckets[index].data[j].sum1 = sum2;
		  buckets[index].data[j].squares1 = square1;

		  buckets[index].data[j].count2 = count2;
		  buckets[index].data[j].sum2 = sum2;
		  buckets[index].data[j].squares2 = square2;

		  buckets[index].data[j].count3 = count3;
		  buckets[index].data[j].sum3 = sum3;
		  buckets[index].data[j].squares3 = square3;

		  buckets[index].data[j].count4 = count4;
		  buckets[index].data[j].sum4 = sum4;

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

	      buckets[index].data[0].count1 = count1;
	      buckets[index].data[0].sum1 = sum1;
	      buckets[index].data[0].squares1 = square1;

	      buckets[index].data[0].count2 = count2;
	      buckets[index].data[0].sum2 = sum2;
	      buckets[index].data[0].squares2 = square2;

	      buckets[index].data[0].count3 = count3;
	      buckets[index].data[0].sum3 = sum3;
	      buckets[index].data[0].squares3 = square3;

	      buckets[index].data[0].count4 = count4;
	      buckets[index].data[0].sum4 = sum4;
	    }

	  /* The current tuple is the start of a run */
	  key = input[start].group;
  
	  sum1 = input[start].value1;
	  square1 = input[start].value1 * input[start].value1;
	  
	  sum2 = input[start].value2;
	  square2 = input[start].value2 * input[start].value2;
	  
	  sum3 = input[start].value3;
	  square3 = input[start].value3 * input[start].value3;
	  
	  sum4 = input[start].value4;
	  square4 = input[start].value4 * input[start].value4;
	  
	  count1 = count2 = count3 = count4 = 1;
	}
    }

  /* flush the last tuple/run */
  AddToGlobalAtomic(a, id, key, 
		    count1, sum1, square1,
		    count2, sum2, square2,
		    count3, sum3, square3,
		    count4, sum4
		    ); 
}
