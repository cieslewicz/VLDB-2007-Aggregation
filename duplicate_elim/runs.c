/* $Id: runs.c,v 1.1 2007/03/17 19:03:02 johnc Exp $
 *
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
				     uint64_t key)
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
	  /* duplicate */
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
  register HashCell *current, *prev, *first;
  /* place oft used info in local variables */  
  register const Tuple* input = a->input;
  
  register HashCell *buckets = a->global_buckets;
  register char *valid = a->valid;

  key = input[start].group;

  for(i = start+1; i <= end; i++)
    {
      if(key != input[i].group)
	{	
	  /* not part of a run */
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
		  /* duplicate */
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
	  key = input[i].group;
	}
    }

  /* flush the last tuple/run */
  AddToGlobalAtomic(a, id, key);
}

/*
 * Aggregate with run optimization, push tuples into the local table.
 */
void AggregateRuns(Aggregate a, const int id, 
		   const int start, const int end)
{
  register unsigned int i, j, k, index;
  register uint64_t key;

  /* place oft used info in local variables */  
  register const Tuple* input = a->input;
  
  //PrivateHashCell **buckets = a->private_buckets;
  //TODO - could we be more specific about what we store here and do
  // PrivateHashCell *buckets = a->private_buckets[id]] ???
  register PrivateHashBucket *buckets = a->private_buckets[id];
 
  key = input[start].group;

  for(i = start+1; i <= end; i++)
    {
      if(key != input[i].group)
	{	
	  /* Run ended, start next run */
	  index = mhash(key, a->lg_private_buckets);	
	  
	  j = 0;
	  while(j < PRIVATE_BUCKET_SIZE 
		&& buckets[index].valid[j] 
		&& buckets[index].data[j].key != key)
	    j++;
	  
	  if(j < PRIVATE_BUCKET_SIZE)
	    {
	      //FOUND key or empty slot
	      if(!buckets[index].valid[j])
		{
		  /* not a duplicate */
		  //Open slot, insert
		  buckets[index].data[j].key = key;
		  buckets[index].valid[j] = 1;
		}
	    }
	  else
	    {
	      // Key not found. Need to evict.
	      // Push the last element into the global table
	      AddToGlobalAtomic(a, id, buckets[index].data[PRIVATE_BUCKET_SIZE - 1].key);
	      
	      // Slide the existing values down, freeing up slot 0
	      for(k = PRIVATE_BUCKET_SIZE - 1; k >0; k --)
		buckets[index].data[k] = buckets[index].data[k-1];
	      
	      // Put the new value in slot 0
	      buckets[index].data[0].key = key;
	    }

	  /* The current tuple is the start of a run */
	  key = input[i].group;
	}
    }

  /* flush the last tuple/run */
  AddToGlobalAtomic(a, id, key);
}
