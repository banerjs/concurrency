/// @file
/// @author Alexander Thomson <thomson@cs.yale.edu>
///
/// Single-threaded performance (tick.zoo.cs.yale.edu, 12/11/11):
///
///    Atomic<int>:
///      Increment: 23.1 ns
///      Assignment: 23.5 ns
///
///    Atomic<ByteArray<1024> >:
///      Assignment: 91.7 ns
///
///    AtomicQueue<int>:
///      Push/Pop: 90.2 ns
///
///    AtomicMap<int, int> [10 elements]:
///      Set/Erase: 299.5 ns
///      Lookup: 60.9 ns
///    AtomicMap<int, int> [1000 elements]:
///      Set/Erase: 290.8 ns
///      Lookup: 56.6 ns
///    AtomicMap<int, int> [1000000 elements]:
///      Set/Erase: 301.4 ns
///      Lookup: 61.5 ns
///

#ifndef _DB_UTILS_ATOMIC_H_
#define _DB_UTILS_ATOMIC_H_

#include <assert.h>
#include <queue>
#include <tr1/unordered_map>

#include "utils/mutex.h"

using std::queue;
using std::tr1::unordered_map;

/// @class AtomicMap<K, V>
///
/// Atomically readable, atomically mutable unordered associative container.
/// Implemented as a std::tr1::unordered_map guarded by a pthread rwlock.
/// Supports CRUD operations only. Iterators are NOT supported.
template<typename K, typename V>
class AtomicMap {
 public:
  AtomicMap() {}

  // Returns the number of key-value pairs currently stored in the map.
  int Size() {
    mutex_.ReadLock();
    int size = map_.size();
    mutex_.Unlock();
    return size;
  }

  // Returns true if the map contains a pair with key equal to 'key'.
  bool Contains(const K& key) {
    mutex_.ReadLock();
    int count = map_.count(key);
    mutex_.Unlock();
    return count > 0;
  }

  // If the map contains a pair with key 'key', sets '*value' equal to the
  // associated value and returns true, else returns false.
  bool Lookup(const K& key, V* value) {
    mutex_.ReadLock();
    if (map_.count(key) != 0) {
      *value = map_[key];
      mutex_.Unlock();
      return true;
    } else {
      mutex_.Unlock();
      return false;
    }
  }

  // Atomically inserts the pair (key, value) into the map (clobbering any
  // previous pair with key equal to 'key'.
  void Insert(const K& key, const V& value) {
    mutex_.WriteLock();
    map_[key] = value;
    mutex_.Unlock();
  }

  // Synonym for 'Insert(key, value)'.
  void Set(const K& key, const V& value) {
    Insert(key, value);
  }

  // Atomically erases any pair with key 'key' from the map.
  void Erase(const K& key) {
    mutex_.WriteLock();
    map_.erase(key);
    mutex_.Unlock();
  }

 private:
  unordered_map<K, V> map_;
  MutexRW mutex_;
};

/// @class AtomicQueue<T>
///
/// Queue with atomic push and pop operations.
///
/// @TODO(alex): This should use lower-contention synchronization.
template<typename T>
class AtomicQueue {
 public:
  AtomicQueue() {}

  // Returns the number of elements currently in the queue.
  void Size() {
    mutex_.Lock();
    int size = queue_.size();
    mutex_.Unlock();
    return size;
  }

  // Atomically pushes 'item' onto the queue.
  void Push(const T& item) {
    mutex_.Lock();
    queue_.push(item);
    mutex_.Unlock();
  }

  // If the queue is non-empty, (atomically) sets '*result' equal to the front
  // element, pops the front element from the queue, and returns true,
  // otherwise returns false.
  bool Pop(T* result) {
    mutex_.Lock();
    if (!queue_.empty()) {
      *result = queue_.front();
      queue_.pop();
      mutex_.Unlock();
      return true;
    } else {
      mutex_.Unlock();
      return false;
    }
  }

  // If mutex is immediately acquired, pushes and returns true, else immediately
  // returns false.
  bool PushNonBlocking(const T& item) {
    if (mutex_.TryLock()) {
      queue_.push(item);
      mutex_.Unlock();
      return true;
    } else {
      return false;
    }
  }

  // If mutex is immediately acquired AND queue is nonempty, pops and returns
  // true, else returns false.
  bool PopNonBlocking(T* result) {
    if (mutex_.TryLock()) {
      if (!queue_.empty()) {
        *result = queue_.front();
        queue_.pop();
        mutex_.Unlock();
        return true;
      } else {
        mutex_.Unlock();
        return false;
      }
    } else {
      return false;
    }
  }

 private:
  queue<T> queue_;
  Mutex mutex_;
};

// An atomically modifiable object. T is required to be a simple numeric type
// or simple struct.
template<typename T>
class Atomic {
 public:
  Atomic() {}
  Atomic(T init) : value_(init) {}

  // Returns the current value.
  T operator* () {
    return value_;
  }

  // Atomically increments the value.
  void operator++ () {
    mutex_.Lock();
    value_++;
    mutex_.Unlock();
  }

  // Atomically increments the value by 'x'.
  void operator+= (T x) {
    mutex_.Lock();
    value_+=x;
    mutex_.Unlock();
  }

  // Atomically decrements the value.
  void operator-- () {
    mutex_.Lock();
    value_--;
    mutex_.Unlock();
  }

  // Atomically decrements the value by 'x'.
  void operator-= (T x) {
    mutex_.Lock();
    value_ -= x;
    mutex_.Unlock();
  }

  // Atomically multiplies the value by 'x'.
  void operator*= (T x) {
    mutex_.Lock();
    value_ *= x;
    mutex_.Unlock();
  }

  // Atomically divides the value by 'x'.
  void operator/= (T x) {
    mutex_.Lock();
    value_ /= x;
    mutex_.Unlock();
  }

  // Atomically %'s the value by 'x'.
  void operator%= (T x) {
    mutex_.Lock();
    value_ %= x;
    mutex_.Unlock();
  }

  // Atomically assigns the value to equal 'x'.
  void operator= (T x) {
    mutex_.Lock();
    value_ = x;
    mutex_.Unlock();
  }

  // Checks if the value is equal to 'old_value'. If so, atomically sets the
  // value to 'new_value' and returns true, otherwise sets '*old_value' equal
  // to the value at the time of the comparison and returns false.
  //
  // TODO(alex): Use C++ <atomic> library to improve performance?
  bool CAS(T* old_value, T new_value) {
    mutex_.Lock();
    if (value_ == *old_value) {
      value_ = new_value;
      mutex_.Unlock();
      return true;
    } else {
      *old_value = value_;
      mutex_.Unlock();
      return false;
    }
  }

 private:
  T value_;
  Mutex mutex_;
};

#endif  // _DB_UTILS_ATOMIC_H_

