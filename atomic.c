/*
 * File: atomic.c
 * Author: John Cieslewicz [johnc@cs.columbia.edu]
 * Copyright (c) 2007 The Trustees of Columbia University
 *
 * This file implements global aggregation using atomic operations.
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

#include <sun_prefetch.h>

/* Insert into the global table */
void AggregateAtomic(Aggregate a, const int id, 
			    const int start, const int end)
{
  register unsigned int i, index;
  register uint64_t key;
  HashCell *current, *prev, *first;

  /* place oft used info in local variables */
  const unsigned int lg_buckets = a->lg_buckets;
  const Tuple* input = a->input;
  register HashCell *buckets = a->global_buckets;
  register char* valid = a->valid;

  for(i = start; i <= end; i++)
    {

      key = input[i].group;

      bool done = false; /* flag set when the current tuple is processed */

      index = mhash(key, lg_buckets);     
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

	      buckets[index].sum1= input[i].value1;
	      buckets[index].count1 = 1;
	      buckets[index].squares1 = input[i].value1 * input[i].value1;

	      buckets[index].sum2 = input[i].value2;
	      buckets[index].count2 = 1;
	      buckets[index].squares2 = input[i].value2 * input[i].value2;

	      buckets[index].sum3 = input[i].value3;
	      buckets[index].count3 = 1;
	      buckets[index].squares3 = input[i].value3 * input[i].value3;

	      buckets[index].sum4 = input[i].value4;
	      buckets[index].count = 1;

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
	  while(current!=NULL && current->key != key)
	    { 
	      prev = current;
	      current = current->next;
	    }
	  
	  if(current)
	    {	     
	      /* Found key -- update aggregate */	      
	      atomic_add_64(&(current->sum1),input[i].value1); /* atomic add */
	      atomic_inc_64(&(current->count1)); /* atomic increment */
	      atomic_add_64(&(current->squares1), input[i].value1 * input[i].value1); /* atomic add */	  

	      atomic_add_64(&(current->sum2),input[i].value2); /* atomic add */
	      atomic_inc_64(&(current->count2)); /* atomic increment */
	      atomic_add_64(&(current->squares2), input[i].value2 * input[i].value2); /* atomic add */

	      atomic_add_64(&(current->sum3),input[i].value3); /* atomic add */
	      atomic_inc_64(&(current->count3)); /* atomic increment */
	      atomic_add_64(&(current->squares3), input[i].value3 * input[i].value3); /* atomic add */

	      atomic_add_64(&(current->sum4),input[i].value4); /* atomic add */
	      atomic_inc_64(&(current->count4)); /* atomic increment */

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

		  current->sum1 = input[i].value1;
		  current->count1 = 1;
		  current->squares1 = input[i].value1 * input[i].value1;

		  current->sum2 = input[i].value2;
		  current->count2 = 1;
		  current->squares2 = input[i].value2 * input[i].value2;

		  current->sum3 = input[i].value3;
		  current->count3 = 1;
		  current->squares3 = input[i].value3 * input[i].value3;

		  current->sum4 = input[i].value4;
		  current->count4 = 1;

		  current->next = first;
		  //		  MUTEX_INIT(current->lock);
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
}
