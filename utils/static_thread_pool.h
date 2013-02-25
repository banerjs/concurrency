/// @file
/// @author Alexander Thomson <thomson@cs.yale.edu>

#ifndef _DB_UTILS_STATIC_THREAD_POOL_H_
#define _DB_UTILS_STATIC_THREAD_POOL_H_

#include "pthread.h"
#include "stdlib.h"
#include "assert.h"
#include <queue>
#include <string>
#include <vector>
#include "utils/atomic.h"
#include "utils/thread_pool.h"

using std::queue;
using std::string;
using std::vector;

//
class StaticThreadPool : public ThreadPool {
 public:
  StaticThreadPool(int nthreads)
      : thread_count_(nthreads), queue_count_(nthreads), stopped_(false) {
    Start();
  }

  StaticThreadPool(int nthreads, int nqueues)
      : thread_count_(nthreads), queue_count_(nqueues), stopped_(false) {
    Start();
  }

  ~StaticThreadPool() {
    stopped_ = true;
    for (int i = 0; i < thread_count_; i++)
      pthread_join(threads_[i], NULL);
  }

  bool Active() { return !stopped_; }

  virtual void RunTask(Task* task) {
    assert(!stopped_);
    while (!queues_[rand() % queue_count_].PushNonBlocking(task)) {}
  }

  virtual int ThreadCount() { return thread_count_; }

 private:
  void Start() {
    threads_.resize(thread_count_);
    queues_.resize(queue_count_);
    for (int i = 0; i < thread_count_; i++) {
      pthread_create(&threads_[i],
                     NULL,
                     RunThread,
                     reinterpret_cast<void*>(this));
    }
  }

  // Function executed by each pthread.
  static void* RunThread(void* arg) {
    StaticThreadPool* tp = reinterpret_cast<StaticThreadPool*>(arg);
    Task* task;
    int sleep_duration = 1;  // in microseconds
    while (true) {
      if (tp->queues_[rand() % tp->queue_count_].PopNonBlocking(&task)) {
        task->Run();
        delete task;
        // Reset backoff.
        sleep_duration = 1;
      } else {
        usleep(sleep_duration);
        // Back off exponentially.
        if (sleep_duration < 32)
          sleep_duration *= 2;
      }

      if (tp->stopped_) {
        // Go through ALL queues looking for a remaining task.
        bool found_task = false;
        int start = rand() % tp->queue_count_;
        for (int i = 0; i < tp->queue_count_; i++) {
          if (tp->queues_[(start + i) % tp->queue_count_].Pop(&task)) {
            found_task = true;
            task->Run();
            delete task;
            break;
          }
        }
        if (!found_task) {
          // All queues are empty.
          break;
        }
      }
    }
    return NULL;
  }

  int thread_count_;
  vector<pthread_t> threads_;

  // Task queues.
  int queue_count_;
  vector<AtomicQueue<Task*> > queues_;

  bool stopped_;
};

#endif  // _DB_UTILS_STATIC_THREAD_POOL_H_

