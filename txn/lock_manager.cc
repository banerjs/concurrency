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
    deque<LockRequest> deq_insert = deque<LockRequest>();   // make new deque
    std::cout<<"new deque made..." <<std::endl;
    deq_insert.push_back(LockRequest(EXCLUSIVE, txn)); // add LockRequest Object to deque
    std::cout<<"LockRequest added to deque..." <<std::endl;
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, &deq_insert)); // add deque to hash
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
      return false;
    }
  }
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
}


// Sets '*owners' to contain the txn IDs of all txns holding the lock, and
// returns the current LockMode of the lock: UNLOCKED if it is not currently
// held, SHARED or EXCLUSIVE if it is, depending on the current state.
LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  std::cout<<"Status: entered function" <<std::endl;
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  std::cout<<"Status: declared lock_deq" <<std::endl;
  if (lock_deq == lock_table_.end()){        // if not found
    return UNLOCKED;
  }
  else{ // if deque found, find all Txn's and add to owners
    std::cout<<"Status - deque found!" <<std::endl;
    for(deque<LockRequest>::iterator l = lock_deq->second->begin();
	l != lock_deq->second->end(); ++l){
      std::cout<<"Status: adding new txn to owners: - " <<l->txn_->ReturnID()<<std::endl;
      owners->push_back(l->txn_);
    }
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

