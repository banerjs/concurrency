// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"
#include <iostream>

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  if (lock_deq == lock_table_.end()){                  // if not found
    deque<LockRequest> *deq_insert = new deque<LockRequest>();   // make new deque
    LockRequest *tr = new LockRequest(EXCLUSIVE, txn);
    deq_insert->push_back(*tr); // add LockRequest Object to deque
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, deq_insert)); // add deque to hash
    return true; // instant lock
  }
  else{ // if deque found, add Lock Request regardless of content
    deque<LockRequest> *d = lock_deq->second;
    d->push_back(LockRequest(EXCLUSIVE, txn));
    // signal instant lock access if deque is empty
    if (d->size() == 1){
      return true;
    }
    // signal no lock yet granted if deque not empty
    else{
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

void LockManagerA::Release(Txn* txn, const Key& key) {
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  if (lock_deq != lock_table_.end()){                // if lock has been issued before    
    deque<LockRequest>::iterator l = lock_deq->second->begin(); // deque holds txn
    if (l == lock_deq->second->end())
      return;
    do {
      if(l->txn_ == txn){// found Lock to Release
	if(l != lock_deq->second->begin()){
	  lock_deq->second->erase(l);
	  txn_waits_.erase(l->txn_);
	  break;
	}
	// pull next LockRequest from erase
	deque<LockRequest>::iterator next = lock_deq->second->erase(l);
	// check if next LockRequest exists
	if (next != lock_deq->second->end()){
	  // we know a next lockrequest exists and is EXCLUSIVE
	  // Therefore decrement that lock in txn_waits_
	  unordered_map<Txn*, int>::iterator new_unlock= txn_waits_.find(next->txn_);  // find newly unlocked txn
	  --new_unlock->second;                      // decrement the lock count
	  if (new_unlock->second == 0){              // if no more locks
	    txn_waits_.erase(new_unlock);            // remove from lockwait deque
	    ready_txns_->push_back(next->txn_);       // add to ready deque
	  }
	}
	break;
      }
      ++l;
    } while (l != lock_deq->second->end());
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  owners->clear(); // clear old lock owners
  if (lock_deq == lock_table_.end()){        // if not found
    return UNLOCKED;
  }
  else{ // if deque found, find all Txn's and add to owners
    deque<LockRequest>::iterator l = lock_deq->second->begin(); // holds txn
    if (l == lock_deq->second->end())
      return UNLOCKED;
    do {
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
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);  
  if (lock_deq == lock_table_.end()){                  // if not found
    std::cout<<"Write:  Key Not Found..." <<std::endl;
    deque<LockRequest> *deq_insert = new deque<LockRequest>();   // make new deque
    std::cout<<"Write:  new deque made..." <<std::endl;
    LockRequest *tr = new LockRequest(EXCLUSIVE, txn);
    deq_insert->push_back(*tr); // add LockRequest Object to deque
    std::cout<<"Write:  LockRequest added to deque..." <<std::endl;
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, deq_insert)); // add deque to hash
    std::cout<<"Write:  inserted deq into hash..." <<std::endl;
    return true; // instant lock
  }
  else{ // if deque found, add Lock Request regardless of content
    std::cout<<"Write:  Got it! - deque found" <<std::endl;
    deque<LockRequest> *d = lock_deq->second;
    d->push_back(LockRequest(EXCLUSIVE, txn));
    std::cout<<"Write:  LockRequest added to deque..." <<std::endl;
    // signal instant lock access if deque is empty
    if (d->size() == 1){
      std::cout<<"Write:  ready for lock! return true" <<std::endl;
      return true;
    }
    // signal no lock yet granted if deque not empty
    else{
      std::cout<<"Write:  NOT ready for lock! return FALSE" <<std::endl;
      // add to txn_waits_ with proper lock count incrementation
      unordered_map<Txn*, int>::iterator no_lock = txn_waits_.find(txn);
      // if not already in wait list add
      if (no_lock == txn_waits_.end()){
	txn_waits_.insert(pair <Txn*, int>(txn,1));
	std::cout<<"Write: new add: txn = "<<txn<<std::endl;
      }
      //else increment 
      else{
	++(no_lock->second);
	std::cout<<"Write: txn = "<<txn<<", and numLocks = "<< no_lock->second<<std::endl;
      }
      return false;
    }
  }
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);  
  if (lock_deq == lock_table_.end()){                  // if not found
    std::cout<<"Read:  Key Not Found..." <<std::endl;
    deque<LockRequest> *deq_insert = new deque<LockRequest>();   // make new deque
    std::cout<<"Read:  new deque made..." <<std::endl;
    LockRequest *tr = new LockRequest(SHARED, txn);
    deq_insert->push_back(*tr); // add LockRequest Object to deque
    std::cout<<"Read:  LockRequest added to deque..." <<std::endl;
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, deq_insert)); // add deque to hash
    std::cout<<"Read:  inserted deq into hash..." <<std::endl;
    return true; // instant lock
  }
  else{ // if deque found, add Lock Request regardless of content
    std::cout<<"Read:  Got it! - deque found" <<std::endl;
    deque<LockRequest> *d = lock_deq->second;
    d->push_back(LockRequest(SHARED, txn));
    std::cout<<"Read:  LockRequest added to deque..." <<std::endl;
    // signal instant lock access if deque is empty
    if (d->size() == 1){
      std::cout<<"Read:  ready for lock! return true" <<std::endl;
      return true;
    }
    // signal no lock yet granted if deque not empty
    else{
      


      std::cout<<"Read:  made share_check deque" <<std::endl;
      for(deque<LockRequest>::iterator  share_check = lock_deq->second->begin();
	  share_check != lock_deq->second->end() && share_check->mode_ != EXCLUSIVE;
	  ++share_check){
	if (share_check->txn_ == txn){
	  std::cout<<"Read:  found txn in start-loop before EXC" <<std::endl;
	  return true; // can grant shared read lock right away
	}
	std::cout<<"Read: looping - shared lock = "<<share_check->txn_<<std::endl;
      }



      std::cout<<"Read:  NOT ready for lock! return FALSE" <<std::endl;
      // add to txn_waits_ with proper lock count incrementation
      unordered_map<Txn*, int>::iterator no_lock = txn_waits_.find(txn);
      // if not already in wait list add
      if (no_lock == txn_waits_.end()){
	txn_waits_.insert(pair <Txn*, int>(txn,1));
	std::cout<<"Read: new add: txn = "<<txn<<std::endl;
      }
      //else increment 
      else{
	++(no_lock->second);
	std::cout<<"Read: txn = "<<txn<<", and numLocks = "<< no_lock->second<<std::endl;
      }
      return false;
    }
  }
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  if (lock_deq != lock_table_.end()){                // if lock has been issued before    
    deque<LockRequest>::iterator l = lock_deq->second->begin(); // deque holds txn
    if (l == lock_deq->second->end())
      return;
    do {
      std::cout<<"Release: checking for unlock: - " <<l->txn_<<std::endl;
      if(l->txn_ == txn){// found Lock to Release
	std::cout<<"Release: Found Lock to release!  -  " <<l->txn_<<std::endl;
	// Not at begining case
	if(l != lock_deq->second->begin()){
	  std::cout<<"--- Release: Not Lock Holder release"<<std::endl;
	  lock_deq->second->erase(l);
	  txn_waits_.erase(l->txn_);
	  return;
	}
	// pull next LockRequest from erase
	deque<LockRequest>::iterator next = lock_deq->second->erase(l);
	// check if next LockRequest exists
	if (next != lock_deq->second->end()){
	  std::cout<<"Release: next lock in line exists!"<<std::endl;	  
	  //Case 1 
	  if(next->mode_ == SHARED && l->mode_ == SHARED){// releasing one of multiple shared lock
	    std::cout<<"--- Release: multiple SHARED Lock release"<<std::endl;
	    lock_deq->second->erase(l);
	    txn_waits_.erase(l->txn_);
	    return;
	  }
	  //Case 2
	  else if(next->mode_ == SHARED && l->mode_ == EXCLUSIVE){// give lock to at least one shared
	    std::cout<<"--- Release: EXCLUSIVE followed by SHARED"<<std::endl;
	    unordered_map<Txn*, int>::iterator new_unlock= txn_waits_.find(next->txn_); // find newly unlocked txn	    
	    do{
	      new_unlock= txn_waits_.find(next->txn_);
	      --new_unlock->second;                      // decrement the lock count
	      if (new_unlock->second == 0){              // if no more locks
		txn_waits_.erase(new_unlock);            // remove from lockwait deque
		ready_txns_->push_back(next->txn_);       // add to ready deque
	      }
 	      ++new_unlock;
	    }
	    while(new_unlock != txn_waits_.end() && next->mode_ == SHARED);
	    return;
	  }
	  //Case 3
	  else if((next->mode_ == EXCLUSIVE && l->mode_ == EXCLUSIVE) ||
		  (next->mode_ == EXCLUSIVE && l->mode_ == SHARED)){
	    std::cout<<"--- Release: EITHER followed by EXCLUSIVE"<<std::endl;
	    unordered_map<Txn*, int>::iterator new_unlock= txn_waits_.find(next->txn_);  // find newly unlocked txn
	    --new_unlock->second;                      // decrement the lock count
	    if (new_unlock->second == 0){              // if no more locks
	      txn_waits_.erase(new_unlock);            // remove from lockwait deque
	      ready_txns_->push_back(next->txn_);       // add to ready deque
	    }
	    return;
	  }
	  //END of Cases
	}
      }
      ++l;
    } while (l != lock_deq->second->end());
  }
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  std::cout<<"Status: entered function" <<std::endl;
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  LockMode mode = EXCLUSIVE;
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
      std::cout<<"Status: adding new txn to owners: - " <<l->txn_<<" - with status = "<<l->mode_<<std::endl;
      owners->push_back(l->txn_);
      if (l->mode_ != mode)
	mode = SHARED;
      ++l;
    } while (l != lock_deq->second->end() && l->mode_ != EXCLUSIVE);
  }
  return mode;
}

