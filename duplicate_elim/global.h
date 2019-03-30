#ifndef _GLOBAL_H_
#define _GLOBAL_H_

/*
 * File: global.h
 * Author: John Cieslewicz [johnc@cs.columbia.edu]
 * Copyright (c) 2007 The Trustees of Columbia University
 *
 * Global definitions go in this file
 */

#include <pthread.h>
#include <thread.h>

#ifndef MAX_THREADS
#define MAX_THREADS (32)
#endif /* MAX_THREADS */

#ifndef L2_CACHE_SIZE
#define L2_CACHE_SIZE (3145728) /* 3MB */
#endif /* L2_CACHE_SIZE */

/* having a bool is nice */
typedef enum {false, true} bool;

/* the input relation is a two column tuple - group by key and value */
typedef struct{
  uint64_t group;
  //  uint64_t value;
}Tuple;

/* * * * * * HASH FUNCTIONS * * * * * * * */


/*
 * Multiplicative hashing
 * k is the 64 bit value to be hashed, tbsize is the lg_2 of the table size
 */
static const uint64_t multiplier = 0xB16538F871F2375D;
static inline uint32_t mhash(const uint64_t k, const int tbsize)
{
  //  const uint64_t multiplier = 0xD5732F178F83561B;
  uint64_t result = k * multiplier;
  return (uint32_t)(result >> (64 - tbsize) );
}


/*
 * This is Jenkin's One-At-A-Time Hash from 
 *  http://en.wikipedia.org/wiki/Hash_table
 * It has been modified to hardcode the size of a uint64_t
 */
static inline uint32_t joaat_hash_hardcoded(unsigned char *key)
{
  uint32_t hash = 0;
  size_t i;
  
  for (i = 0; i < 8; i++) /* note hardcode */
    {
      hash += key[i];
      hash += (hash << 10);
      hash ^= (hash >> 6);
    }
  hash += (hash << 3);
  hash ^= (hash >> 11);
  hash += (hash << 15);
  return hash;
}

/* * * * * * THREADING MACROS * * * * * * */
/* turn off mutexes */
/* #define MUTEX_T pthread_mutex_t */
/* #define MUTEX_LOCK(x) */
/* #define MUTEX_UNLOCK(x)  */
/* #define MUTEX_INIT(x)  */

/* pthread spin locks */
/* #define MUTEX_T pthread_spinlock_t */
/* #define MUTEX_LOCK(a) (pthread_spin_lock(&a)) */
/* #define MUTEX_UNLOCK(a) (pthread_spin_unlock(&a)) */
/* #define MUTEX_INIT(a) (pthread_spin_init(&a, NULL)) */

/* pthread mutexes */
#define MUTEX_T pthread_mutex_t
#define MUTEX_LOCK(x) (pthread_mutex_lock(&x))
#define MUTEX_UNLOCK(x) (pthread_mutex_unlock(&x))
#define MUTEX_INIT(x) (pthread_mutex_init(&x, NULL))


/* solaris mutexes */
/* #define MUTEX_T mutex_t */
/* #define MUTEX_LOCK(a) (mutex_lock(&a)) */
/* #define MUTEX_UNLOCK(a) (mutex_unlock(&a)) */
/* #define MUTEX_INIT(a) (mutex_init(&a, USYNC_THREAD, NULL) ) */

#endif /* _GLOBAL_H_ */
