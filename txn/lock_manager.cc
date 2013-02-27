// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"
#include <iostream>

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

  // Attempts to grant a write lock to the specified transaction, enqueueing
  // request in lock table. Returns true if lock is immediately granted, else
  // returns false.
  //
  // Requires: Neither ReadLock nor WriteLock has previously been called with
  //           this txn and key.
bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  
  if (lock_deq == lock_table_.end()){                  // if not found
    std::cout<<"Key Not Found..." <<std::endl;
    deque<LockRequest> *deq_insert = new deque<LockRequest>();   // make new deque
    std::cout<<"new deque made..." <<std::endl;
    LockRequest *tr = new LockRequest(EXCLUSIVE, txn);
    deq_insert->push_back(*tr); // add LockRequest Object to deque
    std::cout<<"LockRequest added to deque..." <<std::endl;
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, deq_insert)); // add deque to hash
    std::cout<<"inserted deq into hash..." <<std::endl;
    return true; // instant lock
  }
  else{ // if deque found, add Lock Request regardless of content
    std::cout<<"Got it! - deque found" <<std::endl;
    deque<LockRequest> *d = lock_deq->second;
    d->push_back(LockRequest(EXCLUSIVE, txn));
    std::cout<<"LockRequest added to deque..." <<std::endl;
    // signal instant lock access if deque is empty
    if (d->size() == 1){
      std::cout<<"ready for lock! return true" <<std::endl;
      return true;
    }
    // signal no lock yet granted if deque not empty
    else{
      std::cout<<"NOT ready for lock! return FALSE" <<std::endl;
      // add to txn_waits_ with proper lock count incrementation
      unordered_map<Txn*, int>::iterator no_lock = txn_waits_.find(txn);
      // if not already in wait list add
      if (no_lock == txn_waits_.end()){
	txn_waits_.insert(pair <Txn*, int>(txn,1));
      }
      //else increment 
      else{
	++(no_lock->second);
      }
      return false;
    }
  }
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

// Releases lock held by 'txn' on 'key', or cancels any pending request for
// a lock on 'key' by 'txn'. If 'txn' held an EXCLUSIVE lock on 'key' (or was
// the sole holder of a SHARED lock on 'key'), then the next request(s) in the
// request queue is granted. If the granted request(s) corresponds to a
// transaction that has now acquired ALL of its locks, that transaction is
// appended to the 'ready_txns_' queue.
//
// CPSC 438/538:
// IMPORTANT NOTE: In order to know WHEN a transaction is ready to run, you
// may need to track its lock acquisition progress during the lock request
// process.
// (Hint: Use 'LockManager::txn_waits_' defined below.)
void LockManagerA::Release(Txn* txn, const Key& key) {
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);

  if (lock_deq != lock_table_.end()){                // if lock has been issued before    
    deque<LockRequest>::iterator l = lock_deq->second->begin(); // deque holds txn

    if (l == lock_deq->second->end())
      return;

    do {
      std::cout<<"Release: checking for unlock: - " <<l->txn_<<std::endl;

      if(l->txn_ == txn){
	// Release Lock
	std::cout<<"Release: Found Lock to release!  -  " <<l->txn_<<std::endl;
	
	lock_deq->second->erase(l);
	// pull next object from erase, 
	// check if it's real, lock logic
	// decrement in txn_waits
	// if 0, add to ready_txns
	
	break;
      }
      ++l;
    } while (l != lock_deq->second->end() && l->mode_ != EXCLUSIVE);
  }
}


// Sets '*owners' to contain the txn IDs of all txns holding the lock, and
// returns the current LockMode of the lock: UNLOCKED if it is not currently
// held, SHARED or EXCLUSIVE if it is, depending on the current state.
LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  std::cout<<"Status: entered function" <<std::endl;
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  std::cout<<"Status: declared lock_deq" <<std::endl;
  owners->clear(); // clear old lock owners
  if (lock_deq == lock_table_.end()){        // if not found
    return UNLOCKED;
  }
  else{ // if deque found, find all Txn's and add to owners
    std::cout<<"Status - deque found!"<<std::endl;
    deque<LockRequest>::iterator l = lock_deq->second->begin(); // holds txn
    if (l == lock_deq->second->end())
      return UNLOCKED;
    do {
      std::cout<<"Status: adding new txn to owners: - " <<l->txn_<<std::endl;
      owners->push_back(l->txn_);
      ++l;
    } while (l != lock_deq->second->end() && l->mode_ != EXCLUSIVE);
  }
  return EXCLUSIVE;
}


LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  return UNLOCKED;
}

