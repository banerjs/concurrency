// Author: Alexander Thomson (thomson@cs.yale.edu)

#ifndef _STORAGE_H_
#define _STORAGE_H_

#include <limits.h>
#include <tr1/unordered_map>
#include <deque>
#include <map>

#include "txn/common.h"
#include "txn/txn.h"

using std::tr1::unordered_map;
using std::deque;
using std::map;

class Storage {
 public:
  // If there exists a record for the specified key, sets '*result' equal to
  // the value associated with the key and returns true, else returns false;
  bool Read(Key key, Value* result);

  // Inserts the record <key, value>, replacing any previous record with the
  // same key.
  void Write(Key key, Value value);

  // Returns the timestamp at which the record with the specified key was last
  // updated (returns 0 if the record has never been updated).
  double Timestamp(Key key);

 private:
  // Collection of <key, value> pairs.
  unordered_map<Key, Value> data_;

  // Timestamps at which each key was last updated.
  unordered_map<Key, double> timestamps_;
};

#endif  // _STORAGE_H_
