// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

// Definitions for the return value of ReduceLockCount()
#define DONT_EXECUTE -1
#define STILL_WAIT    0
#define OK_EXECUTE    1

// Function definition to use to change the number of locks that a transaction
// is waiting for. If the number of transactions is negative in 'txn_waits_',
// then that implies that the trnsaction has aborted requests for locks before
// it could get all of them. Do not execute in that case. 
inline int ReduceLockCount(int &num_locks) {
  int ret = (num_locks < 0) ? DONT_EXECUTE : OK_EXECUTE;
  num_locks = num_locks - 1;
  return (num_locks) ? STILL_WAIT : ret;
}

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
	pair<Txn*, int> *tp = new pair<Txn*, int>(txn,1);
	txn_waits_.insert(*tp);
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

    while (l != lock_deq->second->end()) {
      if(l->txn_ == txn){  // found Lock to Release
	if(l != lock_deq->second->begin()){  // Does not have the lock now
          unordered_map<Txn*, int>::iterator unlocked = txn_waits_.find(l->txn_);
          if (unlocked != txn_waits_.end()) {  // Mark as a zombie lock req
            unlocked->second = 1 - unlocked->second;
            if (unlocked->second == 0) {  // All zombie requests removed
              txn_waits_.erase(unlocked);
            }
          }
	  lock_deq->second->erase(l);
	  break;
	}
	// pull next LockRequest from erase
	deque<LockRequest>::iterator next = lock_deq->second->erase(l);
	// check if next LockRequest exists
	while (next != lock_deq->second->end()){
	  // we know a next lockrequest exists and is EXCLUSIVE
	  // Therefore decrement that lock in txn_waits_
	  unordered_map<Txn*, int>::iterator new_unlock= txn_waits_.find(next->txn_);  // find newly unlocked txn
	  int returned_value = ReduceLockCount(new_unlock->second);
          if (returned_value == OK_EXECUTE) {       // check if should wait...
            txn_waits_.erase(new_unlock);           // remove from lockwait deque
            ready_txns_->push_back(next->txn_);     // add to ready deque
            break;
          } else if (returned_value == DONT_EXECUTE ||
                     new_unlock->second < 0) {  // all zombie requests removed
            if (returned_value)
              txn_waits_.erase(new_unlock);
            next = lock_deq->second->erase(next);
            continue;
          }
          if (next->mode_ == EXCLUSIVE)  // Always true in this case
            break;
          ++next;
	}
	break;
      }
      ++l;
    }
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
  DERROR("New test has begun\n");
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // if (DEBUG) {
  //   DERROR("The wait list right now is: ");
  //   for (unordered_map<Txn*, int>::iterator it = txn_waits_.begin();
  //        it != txn_waits_.end(); ++it) {
  //     fprintf(stderr,"(0x%lx,%d), ", (unsigned long) it->first, it->second);
  //   }
  //   fprintf(stderr, "\n");
  //   fflush(stderr);
  // }

  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);  

  if (lock_deq == lock_table_.end()){                  // if not found
    deque<LockRequest> *deq_insert = new deque<LockRequest>();   // make new deque
    LockRequest *tr = new LockRequest(EXCLUSIVE, txn);
    deq_insert->push_back(*tr); // add LockRequest Object to deque
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, deq_insert)); // add deque to hash
    return true; // instant lock
  } else { // if deque found, add Lock Request regardless of content
    deque<LockRequest> *d = lock_deq->second;
    LockRequest *tr = new LockRequest(EXCLUSIVE, txn);
    d->push_back(*tr);

    // signal instant lock access if deque is empty
    if (d->size() == 1){
      return true;
    } else {    // signal no lock yet granted if deque not empty
      // add to txn_waits_ with proper lock count incrementation
      unordered_map<Txn*, int>::iterator no_lock = txn_waits_.find(txn);
      // if not already in wait list add
      if (no_lock == txn_waits_.end()){
        DERROR("Wait record missing for transaction 0x%lx\n", (unsigned long) txn);
	pair<Txn*, int> *tp = new pair<Txn*, int>(txn,1);
        txn_waits_.insert(*tp);
      } else {      //else increment
        DERROR("Wait record(%d) found for transaction 0x%lx\n", no_lock->second, (unsigned long) txn);
	++(no_lock->second);
      }
      return false;
    }
  }
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // if (DEBUG) {
  //   DERROR("The wait list right now is: ");
  //   for (unordered_map<Txn*, int>::iterator it = txn_waits_.begin();
  //        it != txn_waits_.end(); ++it) {
  //     fprintf(stderr,"(0x%lx,%d), ", (unsigned long) it->first, it->second);
  //   }
  //   fprintf(stderr, "\n");
  //   fflush(stderr);
  // }
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);  
  if (lock_deq == lock_table_.end()){                  // if not found
    deque<LockRequest> *deq_insert = new deque<LockRequest>();   // make new deque
    LockRequest *tr = new LockRequest(SHARED, txn);
    deq_insert->push_back(*tr); // add LockRequest Object to deque
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, deq_insert)); // add deque to hash
    return true; // instant lock
  } else {  // if deque found, add Lock Request regardless of content
    deque<LockRequest> *d = lock_deq->second;
    LockRequest *tr = new LockRequest(SHARED, txn);
    d->push_back(*tr);
    // signal instant lock access if deque is empty
    if (d->size() == 1){
      return true;
    } else {    // signal no lock yet granted if deque not empty
      for(deque<LockRequest>::iterator  share_check = lock_deq->second->begin();
	  share_check != lock_deq->second->end() && share_check->mode_ != EXCLUSIVE;
	  ++share_check){
	if (share_check->txn_ == txn){
	  return true; // can grant shared read lock right away
	}
      }

      // add to txn_waits_ with proper lock count incrementation
      unordered_map<Txn*, int>::iterator no_lock = txn_waits_.find(txn);
      // if not already in wait list add
      if (no_lock == txn_waits_.end()){
        DERROR("Wait record missing for transaction 0x%lx\n", (unsigned long) txn);
	pair<Txn*, int> *tp = new pair<Txn*, int>(txn,1);
        txn_waits_.insert(*tp);
      } else {      //else increment
        DERROR("Wait record(%d) found for transaction 0x%lx\n", no_lock->second, (unsigned long) txn);
	++(no_lock->second);
      }
      return false;
    }
  }
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // if (DEBUG) {
  //   DERROR("The wait list right now is: ");
  //   for (unordered_map<Txn*, int>::iterator it = txn_waits_.begin();
  //        it != txn_waits_.end(); ++it) {
  //     fprintf(stderr,"(0x%lx,%d), ", (unsigned long) it->first, it->second);
  //   }
  //   fprintf(stderr, "\n");
  //   fflush(stderr);
  // }
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  if (lock_deq == lock_table_.end()){      // if lock has not been issued before
    return;
  }
  
  deque<LockRequest>::iterator l = lock_deq->second->begin(); // deque holds txn

  while (l != lock_deq->second->end()) {  // Find the transaction in the deque
    if(l->txn_ == txn){// found Lock to Release
      
      // The transaction found is at the beginning
      if(l != lock_deq->second->begin()){
        
        if(l->mode_ == EXCLUSIVE){
          deque<LockRequest>::iterator edge_case = lock_deq->second->begin();
          // Cycle through before exclusive to remove checking if all shared
          int all_share = 0;
          for(edge_case = lock_deq->second->begin();edge_case != l; ++edge_case){
            if(edge_case->mode_ == EXCLUSIVE)
              all_share = 1;
          }
          //if all previous are shared
          if(all_share == 0){
            unordered_map<Txn*, int>::iterator new_unlock= txn_waits_.find(l->txn_);  // find newly unlocked txn
            edge_case = l+1;
            while(edge_case!=lock_deq->second->end()) {
              new_unlock = txn_waits_.find(edge_case->txn_);
              if (edge_case->mode_ == EXCLUSIVE && new_unlock->second > 0) {
                break;
              }
              DERROR("EDGE: Wait record(%d) decrementing for transaction 0x%lx\n", new_unlock->second, (unsigned long) edge_case->txn_);
              int returned_value = ReduceLockCount(new_unlock->second); // Decrement
              if (returned_value == OK_EXECUTE){              // if no more locks
                txn_waits_.erase(new_unlock);            // remove from lockwait deque
                ready_txns_->push_back(edge_case->txn_);       // add to ready deque
              } else if (returned_value == DONT_EXECUTE ||  // zombie lock req
                         new_unlock->second < 0) {
                if (returned_value == DONT_EXECUTE)
                  txn_waits_.erase(new_unlock);
                edge_case = lock_deq->second->erase(edge_case);
                continue;
              }
              ++edge_case;
            }
          }
        }
        unordered_map<Txn*, int>::iterator unlocked = txn_waits_.find(l->txn_);
        if (unlocked != txn_waits_.end()) {  // Mark as a zombie lock req
          unlocked->second = 1 - unlocked->second;
          if (unlocked->second == 0) {  // All zombie requests removed
            txn_waits_.erase(unlocked);
          }
        }
        lock_deq->second->erase(l);
        return;
      }
      
      
      // else: these requests are at the beginning of the deque
      // deal with the entry in the txn_waits_ table first
      unordered_map<Txn*, int>::iterator unlocked = txn_waits_.find(l->txn_);
      if (unlocked != txn_waits_.end()) {  // Mark as a zombie lock req
        unlocked->second = 1 - unlocked->second;
        if (unlocked->second == 0) {  // All zombie requests removed
          txn_waits_.erase(unlocked);
        }
      }
      // get rid of req from deque and pull next LockRequest from erase
      deque<LockRequest>::iterator next = lock_deq->second->erase(l);
      DERROR("Just released lock on 0x%lx\n", (unsigned long) l->txn_);

      // check if next LockRequest exists
      if (next != lock_deq->second->end()){
        //Case 2
        if ((l->mode_ == SHARED && next->mode_ == EXCLUSIVE) ||
            l->mode_ == EXCLUSIVE) {  // give lock to at least one shared
          unordered_map<Txn*, int>::iterator new_unlock = txn_waits_.find(next->txn_); // find newly unlocked txn
          
          if (new_unlock == txn_waits_.end()) {  // Should not happen!
            DERROR("Waiting transaction (0x%lx) NOT in wait list\n", (unsigned long) next->txn_);
            DIE("Error in waiting list");
          }
          do {
            DERROR("CASE 2: Wait record(%d) decrementing for transaction 0x%lx\n", new_unlock->second, (unsigned long) next->txn_);
            int returned_value = ReduceLockCount(new_unlock->second); // reduce count
            if (returned_value == OK_EXECUTE) {              // if no more locks
              txn_waits_.erase(new_unlock);             // remove from lockwait deque
              ready_txns_->push_back(next->txn_);       // add to ready deque
              if (next->mode_ == EXCLUSIVE)
                break;
            } else if (returned_value == DONT_EXECUTE ||
                       new_unlock->second < 0) {
              if (returned_value)
                txn_waits_.erase(new_unlock);
              next = lock_deq->second->erase(next); // Remove the zombie request
              continue;
            }
            ++next;
          } while(next != lock_deq->second->end() && next->mode_ == SHARED);
          return;
        }
        //END of Cases
      }
      break;
    }
    ++l;
  }
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  LockMode mode = EXCLUSIVE;
  owners->clear(); // clear old lock owners
  if (lock_deq == lock_table_.end()){        // if not found
    return UNLOCKED;
  }
  else{ // if deque found, find all Txn's and add to owners
    deque<LockRequest>::iterator l = lock_deq->second->begin(); // holds txn
    if (l == lock_deq->second->end())
      return UNLOCKED;

    if(l->mode_ == EXCLUSIVE){
      owners->push_back(l->txn_);
      return EXCLUSIVE;
    }

    while (l != lock_deq->second->end() && l->mode_ != EXCLUSIVE){
      owners->push_back(l->txn_);
      if(l->mode_ != mode){
	mode = SHARED;
      }
      ++l;
    } 
  }
  return mode;
}
