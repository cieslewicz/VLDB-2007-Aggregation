#ifndef _TIMER_H_
#define _TIMER_H_

/*
 * File: timer.h
 * Author: John Cieslewicz [johnc@cs.columbia.edu]
 * Copyright (c) 2007 The Trustees of Columbia University
 *
 * A simple timer ADT that uses the high-res time function 
 * provided by UNIX.
 *
 */

#include <sys/time.h>
#include <stdlib.h>
#include <mtmalloc.h>
#include <assert.h>

/* The Timer structure */
typedef struct TimerCDT
{
  hrtime_t start_time;
  hrtime_t stop_time;
} TimerCDT;

/* The type that the user works with */
typedef TimerCDT *Timer;

/* Return a new Timer struct */
static inline Timer TimerCreate()
{
  Timer t = (Timer)malloc(sizeof(TimerCDT));
  return t;
}

/* Start the timer by recording the current high res time */
static inline void TimerStart(Timer t)
{
  t->start_time = gethrtime();  
}

/* Stop the timer by recording the current high res time */
static inline void TimerStop(Timer t)
{
  t->stop_time = gethrtime();
}

/* Return the elapsed time with a simple calculation */
static inline double TimerElapsed(Timer t)
{
  // note the nanoseconds
  return (t->stop_time - t->start_time)/(1000000000.0);
}

/* Free the Timer data structure */
static inline void TimerDelete(Timer t)
{
  free(t);
}

#endif /*_TIMER_H_*/
