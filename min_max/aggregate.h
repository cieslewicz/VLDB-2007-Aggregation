#ifndef _AGGREGATE_H_
#define _AGGREGATE_H_

/*
 * File: aggregate.h
 * Author: John Cieslewicz [johnc@cs.columbia.edu]
 * Copyright (c) 2007 The Trustees of Columbia University
 */

#include "global.h"

#include <atomic.h>
#include <thread.h>
#include <pthread.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <mtmalloc.h>

#include <libcpc.h>

#define WARMUP 2000
#define SAMPLE_SIZE 1500
#define PRIVATE_BUCKET_SIZE 3

/* The actual aggregate data */
typedef struct AggregateValues
{
  uint64_t key; /* key for this cell */
  uint64_t min; /* accumulated sum for this cell */ 
  uint64_t max; /* accumulated count for this cell */
  uint64_t min2; /* accumulate sum squares for this cell */  
} AggregateValues;

/* The private HashCell structure */
typedef struct PrivateHashBucket
{ /* don't need hit_count any more... */
  unsigned int access_count; /* How many times have we hit this bucket? */
  char valid[PRIVATE_BUCKET_SIZE]; /* has this cell been used? */
  AggregateValues data[PRIVATE_BUCKET_SIZE]; /* The aggregate data */
  int padding[5]; /* Make the bucket equal a full 2 cachelines */
} PrivateHashBucket;

/* The HashCell structure for global tables*/
typedef struct HashCell
{
  volatile uint64_t key; /* key for this cell */
  volatile uint64_t min; /* accumulated sum for this cell */
  volatile uint64_t max; /* accumulated count for this cell */
  volatile uint64_t min2; /* accumulate sum squares for this cell */
  MUTEX_T lock; /* mutual exclusion lock. see global.h */
  struct HashCell *next; /* pointer to the next cell in this chain */

} HashCell;

/* The HashCell structure for global tables*/
typedef struct IndependentHashCell
{
  volatile uint64_t key; /* key for this cell */
  volatile uint64_t min; /* accumulated sum for this cell */
  volatile uint64_t max; /* accumulated count for this cell */
  volatile uint64_t min2; /* accumulate sum squares for this cell */  
  struct IndependentHashCell *next; /* pointer to the next cell in this chain */
  char valid;
  int padding[4];
} IndependentHashCell;

/* The Concrete datatype that holds aggregation data */
typedef struct AggregateCDT
{
  Tuple *input;  /* the input relation. see global.h */
  HashCell *global_buckets; /* The global hash table */
  PrivateHashBucket **private_buckets; /* The local tables */  
  IndependentHashCell **independent_cells;

  char *valid; /* byte vector for determining if buckets are valid */

  unsigned int n_private_buckets; /* The number of buckets in the local table */
  unsigned int n_buckets; /* number of buckets in the hash table */
  unsigned int n_threads; /* number of threads to use while doing the aggregation */
  unsigned int n_tups; /* the number of tuples in the input relation */
  unsigned int lg_buckets;
  unsigned int lg_private_buckets;
  unsigned int hits[MAX_THREADS];
  unsigned int accesses[MAX_THREADS];
  unsigned int resample_rate;
  unsigned int n_partitions;
  unsigned int current_partition;
} AggregateCDT;

typedef struct AggregateCDT *Aggregate;

/* Information that we need to pass to each thread */
typedef struct ThreadInfo
{
  int id;
  Aggregate a; /* Pointer to the aggregate structure */
#ifdef _PROFILE_
  cpc_t* cpc;
  char * event;
  uint64_t event_cnt;
  uint64_t instr_cnt;
#endif /* _PROFILE_ */
} ThreadInfo;


/* * * Functions for Clients * * */

extern Aggregate AggregateCreate(int n_threads, Tuple* tups, int n_tups, int n_groups, int resample_rate);

extern double AggregateRun(Aggregate a);

extern double AggregateMerge(Aggregate a);

extern void AggregatePrint(Aggregate a);

extern void AggregateDelete(Aggregate a);

extern void AggregateReset(Aggregate a);

extern double AggregateMissRate(Aggregate a);

/* * * Internal stuff  * * */
/* TODO these functions, plus structures above could reside in a different header */
extern Aggregate InitializeAggregate(int n_threads, Tuple *tups, int n_tups, 
				       int n_groups);

extern void InitializePrivateTables(Aggregate a);

extern void AggregateAtomic(Aggregate a, const int id, 
			    const int start, const int end);

extern void AggregateMutex(Aggregate a, const int id, 
			   const int start, const int end);

extern void AggregateHybrid(Aggregate a, const int id, 
			  const int start, const int end);

extern void AggregateSample(Aggregate a, const int id, 
			    const int start, const int end, 
			    int *hits, int* num_runs);

extern void AggregateMergeLite(Aggregate a, const int id);

extern void AggregateRuns(Aggregate a, const int id, 
		   const int start, const int end);

extern void AggregateRunsGlobal(Aggregate a, const int id, 
		   const int start, const int end);

extern void DeleteGlobalTable(Aggregate a);

extern void ResetGlobalTable(Aggregate a);

extern void ResetPrivateTables(Aggregate a);

#endif /* _AGGREGATE_H_ */
