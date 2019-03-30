/*
 * File: hybrid.c
 * Author: John Cieslewicz [johnc@cs.columbia.edu]
 * Copyright (c) 2007 The Trustees of Columbia University
 *
 * This code initializes/resets the private tables. 
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
