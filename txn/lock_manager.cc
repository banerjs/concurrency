// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"


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
    txn_waits_.insert(pair<Txn*, int>(txn, 0)); // Notify the system that txn is alive
    return true; // instant lock
  }
  else{ // if deque found, add Lock Request regardless of content
    deque<LockRequest> *d = lock_deq->second;
    d->push_back(LockRequest(EXCLUSIVE, txn));
    // signal instant lock access if deque is empty
    if (d->size() == 1) {
      txn_waits_.insert(pair<Txn*, int>(txn, 0)); // Notify system that the txn is alive
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
          unordered_map<Txn*, int>::iterator zombie = txn_waits_.find(txn);
          if (zombie != txn_waits_.end()) {  // This is not a zombie request
            txn_waits_.erase(txn);  // Make the txn a zombie
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
	  unordered_map<Txn*, int>::iterator zombie= txn_waits_.find(next->txn_);  // check for zombie
          if (zombie == txn_waits_.end()) {  // This transaction is a zombie
            next = lock_deq->second->erase(next); // Remove the zombie
            continue;
          } else if (zombie->second > 0) {  // Valid txn waiting
            --(zombie->second);
            if (zombie->second == 0) {  // Has all the locks now
              ready_txns_->push_back(next->txn_);
            }
          } else {  // zombie->second == 0. Found a txn with all locks waiting
            DIE("Invalid Txn. Waiting but with all locks!"); // Maybe do nothing
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

  // First check for req after release
  // unordered_map<Txn*, int>::iterator found = txn_waits_.find(txn);
  // if (found != txn_waits_.end() && found->second < 0) {  // In the release phase
  //   DERROR("0x%lx Requesting lock after release(%d)!\n", (unsigned long) txn, found->second);
  //   return false;
  // }

  // unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);  

  // if (lock_deq == lock_table_.end()){                  // if not found
  //   deque<LockRequest> *deq_insert = new deque<LockRequest>();   // make new deque
  //   LockRequest *tr = new LockRequest(EXCLUSIVE, txn);
  //   deq_insert->push_back(*tr); // add LockRequest Object to deque
  //   lock_table_.insert(pair<Key, deque<LockRequest>*>(key, deq_insert)); // add deque to hash
  //   return true; // instant lock
  // } else { // if deque found, add Lock Request regardless of content
  //   deque<LockRequest> *d = lock_deq->second;
  //   LockRequest *tr = new LockRequest(EXCLUSIVE, txn);
  //   d->push_back(*tr);

  //   // signal instant lock access if deque is empty
  //   if (d->size() == 1){
  //     return true;
  //   } else {    // signal no lock yet granted if deque not empty
  //     // add to txn_waits_ with proper lock count incrementation
  //     unordered_map<Txn*, int>::iterator no_lock = txn_waits_.find(txn);
  //     // if not already in wait list add
  //     if (no_lock == txn_waits_.end()){
  //       DERROR("Wait record missing for transaction 0x%lx\n", (unsigned long) txn);
  //       pair<Txn*, int> *tp = new pair<Txn*, int>(txn,1);
  //       txn_waits_.insert(*tp);
  //     } else {      //else increment
  //       DERROR("Wait record(%d) found for transaction 0x%lx\n", no_lock->second, (unsigned long) txn);
  //       ++(no_lock->second);
  //     }
  //     return false;
  //   }
  // }
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  if (lock_deq == lock_table_.end()){                  // if not found
    deque<LockRequest> *deq_insert = new deque<LockRequest>();   // make new deque
    LockRequest *tr = new LockRequest(EXCLUSIVE, txn);
    deq_insert->push_back(*tr); // add LockRequest Object to deque
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, deq_insert)); // add deque to hash
    txn_waits_.insert(pair<Txn*, int>(txn, 0)); // Notify the system that txn is alive
    return true; // instant lock
  } else { // if deque found, add Lock Request regardless of content
    deque<LockRequest> *d = lock_deq->second;
    d->push_back(LockRequest(EXCLUSIVE, txn));
    // signal instant lock access if deque is empty
    if (d->size() == 1) {
      txn_waits_.insert(pair<Txn*, int>(txn, 0)); // Notify system that the txn is alive
      return true;
    } else {  // signal no lock yet granted if deque not empty
      // add to txn_waits_ with proper lock count incrementation
      unordered_map<Txn*, int>::iterator no_lock = txn_waits_.find(txn);
      // if not already in wait list add
      if (no_lock == txn_waits_.end()){
	pair<Txn*, int> *tp = new pair<Txn*, int>(txn,1);
	txn_waits_.insert(*tp);
      } else {  //else increment 
	++(no_lock->second);
      }
      return false;
    }
  }

  // Never reached
  return true;
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

  // // First check for req after release
  // unordered_map<Txn*, int>::iterator found = txn_waits_.find(txn);
  // if (found != txn_waits_.end() && found->second < 0) {  // In the release phase
  //   return false;
  // }

  // unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);  
  // if (lock_deq == lock_table_.end()){                  // if not found
  //   deque<LockRequest> *deq_insert = new deque<LockRequest>();   // make new deque
  //   LockRequest *tr = new LockRequest(SHARED, txn);
  //   deq_insert->push_back(*tr); // add LockRequest Object to deque
  //   lock_table_.insert(pair<Key, deque<LockRequest>*>(key, deq_insert)); // add deque to hash
  //   return true; // instant lock
  // } else {  // if deque found, add Lock Request regardless of content
  //   deque<LockRequest> *d = lock_deq->second;
  //   LockRequest *tr = new LockRequest(SHARED, txn);
  //   d->push_back(*tr);
  //   // signal instant lock access if deque is empty
  //   if (d->size() == 1){
  //     return true;
  //   } else {    // signal no lock yet granted if deque not empty
  //     for(deque<LockRequest>::iterator  share_check = lock_deq->second->begin();
  //         share_check != lock_deq->second->end() && share_check->mode_ != EXCLUSIVE;
  //         ++share_check){
  //       if (share_check->txn_ == txn){
  //         return true; // can grant shared read lock right away
  //       }
  //     }

  //     // add to txn_waits_ with proper lock count incrementation
  //     unordered_map<Txn*, int>::iterator no_lock = txn_waits_.find(txn);
  //     // if not already in wait list add
  //     if (no_lock == txn_waits_.end()){
  //       DERROR("Wait record missing for transaction 0x%lx\n", (unsigned long) txn);
  //       pair<Txn*, int> *tp = new pair<Txn*, int>(txn,1);
  //       txn_waits_.insert(*tp);
  //     } else {      //else increment
  //       DERROR("Wait record(%d) found for transaction 0x%lx\n", no_lock->second, (unsigned long) txn);
  //       ++(no_lock->second);
  //     }
  //     return false;
  //   }
  // }
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  if (lock_deq == lock_table_.end()){                  // if not found
    deque<LockRequest> *deq_insert = new deque<LockRequest>();   // make new deque
    LockRequest *tr = new LockRequest(SHARED, txn);
    deq_insert->push_back(*tr); // add LockRequest Object to deque
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, deq_insert)); // add deque to hash
    txn_waits_.insert(pair<Txn*, int>(txn, 0)); // Notify the system that txn is alive
    return true; // instant lock
  } else { // if deque found, add Lock Request regardless of content
    deque<LockRequest> *d = lock_deq->second;
    d->push_back(LockRequest(SHARED, txn));
    // signal instant lock access if deque is empty
    if (d->size() == 1) {
      txn_waits_.insert(pair<Txn*, int>(txn, 0)); // Notify system that the txn is alive
      return true;
    } else {  // signal no lock yet granted if deque not empty
      for(deque<LockRequest>::iterator share_check = lock_deq->second->begin();
          share_check != lock_deq->second->end() && share_check->mode_ != EXCLUSIVE;
          ++share_check){
        if (share_check->txn_ == txn){  // Only shared locks held on key so far
          unordered_map<Txn*, int>::iterator found = txn_waits_.find(txn);
          if (found == txn_waits_.end()) {  // This txn does not exist yet
            txn_waits_.insert(pair<Txn*, int>(txn, 0)); // Notify system not a
                                                        // zombie
          }  // else no need to do anything
          return true; // can grant shared read lock right away
        }
      }
      
      // else there is something blocking txn from getting a lock. Act
      // appropriately
      unordered_map<Txn*, int>::iterator found = txn_waits_.find(txn);
      if (found == txn_waits_.end()) {  // System thinks txn is a zombie
        txn_waits_.insert(pair<Txn*, int>(txn, 1));
      } else {  // Increment the number of locks this txn needs
        ++(found->second);
      }
      return false;
    }
  }

  // Never reached
  return true;
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

  // DERROR("Releasing the following K,V pair: %lu, 0x%lx\n", key, (unsigned long) txn);

  // unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  // if (lock_deq == lock_table_.end()){      // if lock has not been issued before
  //   return;
  // }
  
  // deque<LockRequest>::iterator l = lock_deq->second->begin(); // deque holds txn

  // while (l != lock_deq->second->end()) {  // Find the transaction in the deque
  //   if(l->txn_ == txn){// found Lock to Release
      
  //     // The transaction found is at the beginning
  //     if(l != lock_deq->second->begin()){
        
  //       if(l->mode_ == EXCLUSIVE){
  //         deque<LockRequest>::iterator edge_case = lock_deq->second->begin();
  //         // Cycle through before exclusive to remove checking if all shared
  //         int all_share = 0;
  //         for(edge_case = lock_deq->second->begin();edge_case != l; ++edge_case){
  //           if(edge_case->mode_ == EXCLUSIVE)
  //             all_share = 1;
  //         }
  //         //if all previous are shared
  //         if(all_share == 0){
  //           unordered_map<Txn*, int>::iterator new_unlock= txn_waits_.find(l->txn_);  // find newly unlocked txn
  //           edge_case = l+1;
  //           while(edge_case!=lock_deq->second->end()) {
  //             new_unlock = txn_waits_.find(edge_case->txn_);
  //             if (edge_case->mode_ == EXCLUSIVE && new_unlock->second > 0) {
  //               break;
  //             }
  //             DERROR("%d: Wait record(%d) decrementing for transaction 0x%lx, %d\n", __LINE__, new_unlock->second, (unsigned long) edge_case->txn_, edge_case->mode_);
  //             int returned_value = ReduceLockCount(new_unlock->second); // Decrement
  //             DERROR("%d: Wait record(%d) is now associated(%d) for transaction 0x%lx, %d\n", __LINE__, new_unlock->second, returned_value, (unsigned long) edge_case->txn_, edge_case->mode_);
  //             if (returned_value == OK_EXECUTE){              // if no more locks
  //               DERROR("%d: Executing. Bye bye 0x%lx, %d\n", __LINE__, (unsigned long) edge_case->txn_, edge_case->mode_);
  //               txn_waits_.erase(new_unlock);            // remove from lockwait deque
  //               ready_txns_->push_back(edge_case->txn_);       // add to ready deque
  //             } else if (returned_value == DONT_EXECUTE ||  // zombie lock req
  //                        new_unlock->second < 0) {
  //               if (returned_value == DONT_EXECUTE) {
  //                 DERROR("%d: Dying.Bye bye 0x%lx, %d\n", __LINE__, (unsigned long) edge_case->txn_, edge_case->mode_);
  //                 txn_waits_.erase(new_unlock);
  //               }
  //               edge_case = lock_deq->second->erase(edge_case);
  //               continue;
  //             }
  //             ++edge_case;
  //           }
  //         }
  //       }
  //       unordered_map<Txn*, int>::iterator unlocked = txn_waits_.find(l->txn_);
  //       if (unlocked != txn_waits_.end()) {  // Mark as a zombie lock req
  //         if (unlocked->second > 0)
  //           unlocked->second = 1 - unlocked->second;
  //         else
  //           ReduceLockCount(unlocked->second);
  //         if (unlocked->second == 0) {  // All zombie requests removed
  //           DERROR("%d: Dying. Bye bye 0x%lx, %d\n", __LINE__, (unsigned long) l->txn_, l->mode_);
  //           txn_waits_.erase(unlocked);
  //         }
  //       }
  //       lock_deq->second->erase(l);
  //       return;
  //     }
      
      
  //     // else: these requests are at the beginning of the deque
  //     // deal with the entry in the txn_waits_ table first
  //     unordered_map<Txn*, int>::iterator unlocked = txn_waits_.find(l->txn_);
  //     if (unlocked != txn_waits_.end()) {  // Mark as a zombie lock req
  //       if (unlocked->second > 0)
  //         unlocked->second = 1 - unlocked->second;
  //       else
  //         ReduceLockCount(unlocked->second);
  //       if (unlocked->second == 0) {  // All zombie requests removed
  //         DERROR("%d: Dying. Bye bye 0x%lx, %d\n", __LINE__, (unsigned long) l->txn_, l->mode_);
  //         txn_waits_.erase(unlocked);
  //       }
  //     }
  //     // get rid of req from deque and pull next LockRequest from erase
  //     deque<LockRequest>::iterator next = lock_deq->second->erase(l);

  //     // check if next LockRequest exists
  //     if (next != lock_deq->second->end()){
  //       //Case 2
  //       if ((l->mode_ == SHARED && next->mode_ == EXCLUSIVE) ||
  //           l->mode_ == EXCLUSIVE) {  // give lock to at least one shared
  //         unordered_map<Txn*, int>::iterator new_unlock = txn_waits_.find(next->txn_); // find newly unlocked txn
          
  //         if (new_unlock == txn_waits_.end()) {  // Should not happen!
  //           DERROR("Waiting transaction (0x%lx) NOT in wait list\n", (unsigned long) next->txn_);
  //           DIE("Error in waiting list");
  //         }
  //         while(next != lock_deq->second->end()) {
  //           DERROR("%d: Wait record(%d) decrementing for transaction 0x%lx, %d\n", __LINE__, new_unlock->second, (unsigned long) next->txn_, next->mode_);
  //           int returned_value = ReduceLockCount(new_unlock->second); // reduce count
  //           DERROR("%d: Wait record(%d) is now associated(%d) for transaction 0x%lx, %d\n", __LINE__, new_unlock->second, returned_value, (unsigned long) next->txn_, next->mode_);            
  //           if (returned_value == OK_EXECUTE) {              // if no more locks
  //             DERROR("%d: Executing. Bye bye 0x%lx, %d\n", __LINE__, (unsigned long) next->txn_, next->mode_);
  //             txn_waits_.erase(new_unlock);             // remove from lockwait deque
  //             ready_txns_->push_back(next->txn_);       // add to ready deque
  //           } else if (returned_value == DONT_EXECUTE ||
  //                      new_unlock->second < 0) {
  //             if (returned_value == DONT_EXECUTE) {
  //               DERROR("%d: Dying. Bye bye 0x%lx, %d\n", __LINE__, (unsigned long) next->txn_, next->mode_);
  //               txn_waits_.erase(new_unlock);
  //             }
  //             next = lock_deq->second->erase(next); // Remove the zombie request
  //             continue;
  //           }
  //           if (next->mode_ == EXCLUSIVE)  // Check for the first iteration
  //             break;
  //           ++next;
  //           if (next != lock_deq->second->end() && next->mode_ == EXCLUSIVE)  // Check for all following iterations
  //             break;
  //         }
  //         return;
  //       }
  //       //END of Cases
  //     }
  //     break;
  //   }
  //   ++l;
  // }
  DERROR("Releasing 0x%lx\'s lock on %lu", (unsigned long) txn, key);

  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq = lock_table_.find(key);
  if (lock_deq == lock_table_.end()) {    // if lock has not been issued before
    return;                               //   lock does not exist
  }

  deque<LockRequest>::iterator l = lock_deq->second->begin(); // deque holds txn
  while (l != lock_deq->second->end()) {  // while there are txns to loop over
    if (l->txn_ != txn) {  // Continue to beginning of loop if txn not found
      ++l;                 // Increment the position of the iterator
      continue;            // Bypass rest of the loop
    }

    // else the transaction has been found in this section of the loop
    if (l == lock_deq->second->begin()) {  // This is the beginning of the deque
      // Mark the transaction as a zombie
      unordered_map<Txn*, int>::iterator found = txn_waits_.find(txn);
      if (found != txn_waits_.end()) {  // System thinks txn is alive
        txn_waits_.erase(txn);          //   Prove it wrong!
      }                                 // Else do nothing to wait-list

      // Remove the txn entry from the lock table
      deque<LockRequest>::iterator next = lock_deq->second->erase(l);

      // Check to see which txn to set ready next
      while (next != lock_deq->second->end()) {  // While there are txns
        // If txn was am exclusive txn, then check the next txn
      }
    } else {                               // Else middle of the deque
    }                                      // end else (middle of deque)
    break;  // Do not continue with the loop. Transaction has been handled
  }

  return;
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
