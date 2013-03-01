// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"


LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq =
    lock_table_.find(key);
  if (lock_deq == lock_table_.end()) {                          // If not found
    deque<LockRequest> *deq_insert = new deque<LockRequest>();  // New deque
    LockRequest *tr = new LockRequest(EXCLUSIVE, txn);
    deq_insert->push_back(*tr);                           // add LockRequest
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, deq_insert));
    txn_waits_.insert(pair<Txn*, int>(txn, 0));           // Notify txn is alive
    return true;                                          // instant lock
  } else {  // if deque found, add Lock Request regardless of content
    deque<LockRequest> *d = lock_deq->second;
    d->push_back(LockRequest(EXCLUSIVE, txn));
    // signal instant lock access if deque is empty
    if (d->size() == 1) {
      txn_waits_.insert(pair<Txn*, int>(txn, 0));         // Notify txn is alive
      return true;
    } else {  // signal no lock yet granted if deque not empty
      // add to txn_waits_ with proper lock count incrementation
      unordered_map<Txn*, int>::iterator no_lock = txn_waits_.find(txn);
      // if not already in wait list add
      if (no_lock == txn_waits_.end()) {
        pair<Txn*, int> *tp = new pair<Txn*, int>(txn, 1);
        txn_waits_.insert(*tp);
      } else {  // else increment
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
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq =
    lock_table_.find(key);
  if (lock_deq != lock_table_.end()) {  // if lock has been issued before
    deque<LockRequest>::iterator l =  // deque holds txn
      lock_deq->second->begin();
    while (l != lock_deq->second->end()) {
      if (l->txn_ == txn) {  // found Lock to Release
        if (l != lock_deq->second->begin()) {  // Does not have the lock now
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
        while (next != lock_deq->second->end()) {
          // we know a next lockrequest exists and is EXCLUSIVE
          // Therefore decrement that lock in txn_waits_
          unordered_map<Txn*, int>::iterator zombie=
            txn_waits_.find(next->txn_);  // check for zombie
          if (zombie == txn_waits_.end()) {  // This transaction is a zombie
            next = lock_deq->second->erase(next);  // Remove the zombie
            continue;
          } else if (zombie->second > 0) {  // Valid txn waiting
            --(zombie->second);
            if (zombie->second == 0) {  // Has all the locks now
              ready_txns_->push_back(next->txn_);
            }
          } else {  // zombie->second == 0. Found a txn with all locks waiting
            DIE("Invalid Txn. Waiting but with all locks!");
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
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq =
    lock_table_.find(key);
  owners->clear();  // clear old lock owners
  if (lock_deq == lock_table_.end()) {  // if not found
    return UNLOCKED;
  } else {  // if deque found, find all Txn's and add to owners
    deque<LockRequest>::iterator l = lock_deq->second->begin();  // holds txn
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
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq
    = lock_table_.find(key);
  if (lock_deq == lock_table_.end()) {  // if not found
    deque<LockRequest> *deq_insert =
      new deque<LockRequest>();   // make new deque
    LockRequest *tr = new LockRequest(EXCLUSIVE, txn);
    deq_insert->push_back(*tr);  // add LockRequest Object to deque
    // add deque to hash and notify the system that txn is alive
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, deq_insert));
    txn_waits_.insert(pair<Txn*, int>(txn, 0));
    return true;  // instant lock
  } else {  // if deque found, add Lock Request regardless of content
    deque<LockRequest> *d = lock_deq->second;
    d->push_back(LockRequest(EXCLUSIVE, txn));
    // signal instant lock access if deque is empty
    if (d->size() == 1) {
      // Notify system that the txn is alive
      txn_waits_.insert(pair<Txn*, int>(txn, 0));
      return true;
    } else {  // signal no lock yet granted if deque not empty
      // add to txn_waits_ with proper lock count incrementation
      unordered_map<Txn*, int>::iterator no_lock =
        txn_waits_.find(txn);
      // if not already in wait list add
      if (no_lock == txn_waits_.end()) {
        pair<Txn*, int> *tp = new pair<Txn*, int>(txn, 1);
        txn_waits_.insert(*tp);
      } else {  // else increment
        ++(no_lock->second);
      }
      return false;
    }
  }

  // Never reached
  return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq =
    lock_table_.find(key);
  if (lock_deq == lock_table_.end()) {  // if not found
    deque<LockRequest> *deq_insert =
      new deque<LockRequest>();  // make new deque
    LockRequest *tr = new LockRequest(SHARED, txn);
    deq_insert->push_back(*tr);  // add LockRequest Object to deque
    // add deque to hash and notify system that txn is alive
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, deq_insert));
    txn_waits_.insert(pair<Txn*, int>(txn, 0));
    return true;  // instant lock
  } else {  // if deque found, add Lock Request regardless of content
    deque<LockRequest> *d = lock_deq->second;
    d->push_back(LockRequest(SHARED, txn));
    // signal instant lock access if deque is empty
    if (d->size() == 1) {
      // Notify system that the txn is alive
      txn_waits_.insert(pair<Txn*, int>(txn, 0));
      return true;
    } else {  // signal no lock yet granted if deque not empty
      for (deque<LockRequest>::iterator share_check = lock_deq->second->begin();
          share_check != lock_deq->second->end() &&
             share_check->mode_ != EXCLUSIVE;
          ++share_check) {
        if (share_check->txn_ == txn) {  // Only shared locks held on key so far
          unordered_map<Txn*, int>::iterator found = txn_waits_.find(txn);
          if (found == txn_waits_.end()) {  // This txn does not exist yet
            txn_waits_.insert(pair<Txn*, int>(txn, 0));  // Notify system not a
                                                        // zombie
          }  // else no need to do anything
          return true;  // can grant shared read lock right away
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
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq =
    lock_table_.find(key);
  if (lock_deq == lock_table_.end()) {  // if lock has not been issued before
    return;                             //   lock does not exist
  }

  deque<LockRequest>::iterator l =
    lock_deq->second->begin();  // deque holds txn
  while (l != lock_deq->second->end()) {  // while there are txns to loop over
    if (l->txn_ != txn) {  // Continue to beginning of loop if txn not found
      ++l;                 // Increment the position of the iterator
      continue;            // Bypass rest of the loop
    }

    LockMode txn_mode = l->mode_;  // Save the mode for later use

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
        // If txn was an exclusive txn, then check the next txn, and grant locks
        // until another exclusive lock request. Else if the lock was the last
        // shared, then grant the immediate next one (unless its a
        // zombie). Otherwise don't do anything because it's been taken care of
        // elsewhere.
        if (txn_mode == EXCLUSIVE) {  // Grant the following txns
          unordered_map<Txn*, int>::iterator unlock =
            txn_waits_.find(next->txn_);
          if (unlock == txn_waits_.end()) {  // Found a zombie
            next = lock_deq->second->erase(next);  // Update iterator
            continue;
          } else {  // Grant the lock to the txn
            --(unlock->second);  // Reduce the lock_wait count
            if (unlock->second == 0) {  // All locks granted
              ready_txns_->push_back(next->txn_);  // State txn as ready
            }  // else continue
          }
          if (next->mode_ == EXCLUSIVE) {  // Exclusive txn so break
            break;
          } else {  // Increment and check for exclusive
            ++next;
            if (next != lock_deq->second->end() &&
                next->mode_ == EXCLUSIVE)
              break;
          }
        } else {  // txn_mode == SHARED. This transaction was a shared
          if (next->mode_ == SHARED) {  // Deal with edge case of zombie X lock
            unordered_map<Txn*, int>::iterator unlock =
              txn_waits_.find(next->txn_);
            if (unlock->second == 0) {  // Probably already has a lock. So break
              break;
            } else {
              --(unlock->second);  // Mark as locked
              if (unlock->second == 0) {  // Send to ready
                ready_txns_->push_back(next->txn_);
              }

              // Increment next and check for more locks
              ++next;
              if (next != lock_deq->second->end() &&
                  next->mode_ == EXCLUSIVE)
                break;
            }
          } else {  // Check for zombie, else send to ready and break
            unordered_map<Txn*, int>::iterator found =
              txn_waits_.find(next->txn_);
            if (found == txn_waits_.end()) {  // found a zombie
              next = lock_deq->second->erase(next);  // Update next
              continue;
            } else {  // This is valid transaction and hence break
              --(found->second);  // Mark as locked
              if (found->second == 0) {  // Send to ready
                ready_txns_->push_back(next->txn_);
              }
              break;
            }
          }
        }
      }
    } else {                               // Else middle of the deque
      if (txn_mode == EXCLUSIVE) {   // Edge case of possible X btw. S
        bool share_sandwich = true;  // Flag for S sandwich
        for (deque<LockRequest>::iterator dummy = lock_deq->second->begin();
             dummy != l; ++dummy) {  // Loop to check for valid S's
          if (dummy->mode_ == EXCLUSIVE) {  // Sandwich impossible :(
            share_sandwich = false;
            break;
          }
        }
        if (share_sandwich) {        // We have a sandwich. Oh Joy!
          unordered_map<Txn*, int>::iterator zombie = txn_waits_.find(txn);
          if (zombie != txn_waits_.end()) {  // System thinks txn is alive
            txn_waits_.erase(txn);           //   Prove it wrong!
          }                                  // Else do nothing to wait-list

          // Remove the txn from the deque
          deque<LockRequest>::iterator next = lock_deq->second->erase(l);
          while (next != lock_deq->second->end() && next->mode_ == SHARED) {
            unordered_map<Txn*, int>::iterator found =
              txn_waits_.find(next->txn_);
            if (found == txn_waits_.end()) {  // Found a zombie
              next = lock_deq->second->erase(next);  // Update next
              continue;
            } else {
              --(found->second);  // Mark as locked
              if (found->second == 0) {  // Send to ready
                ready_txns_->push_back(next->txn_);
              }
              ++next;
            }
          }
        } else {                     // No sandwich. So remove the stuff
          unordered_map<Txn*, int>::iterator found = txn_waits_.find(txn);
          if (found != txn_waits_.end()) {  // System thinks txn is alive
            txn_waits_.erase(txn);          //   Prove it wrong!
          }                                 // Else do nothing to wait-list

          // Remove the txn entry from the lock table
          lock_deq->second->erase(l);
        }
      } else {  // txn_mode == SHARED. Just remove it
        unordered_map<Txn*, int>::iterator found = txn_waits_.find(txn);
        if (found != txn_waits_.end()) {  // System thinks txn is alive
          txn_waits_.erase(txn);          //   Prove it wrong!
        }                                 // Else do nothing to wait-list

        // Remove the txn entry from the lock table
        lock_deq->second->erase(l);
      }
    }                                      // end else (middle of deque)
    break;  // Do not continue with the loop. Transaction has been handled
  }

  return;
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  unordered_map<Key, deque<LockRequest>*>::iterator lock_deq =
    lock_table_.find(key);
  LockMode mode = EXCLUSIVE;
  owners->clear();  // clear old lock owners
  if (lock_deq == lock_table_.end()) {  // if not found
    return UNLOCKED;
  } else {  // if deque found, find all Txn's and add to owners
    deque<LockRequest>::iterator l =
      lock_deq->second->begin();  // holds txn
    if (l == lock_deq->second->end())
      return UNLOCKED;

    if (l->mode_ == EXCLUSIVE) {
      owners->push_back(l->txn_);
      return EXCLUSIVE;
    }

    while (l != lock_deq->second->end() && l->mode_ != EXCLUSIVE) {
      owners->push_back(l->txn_);
      if (l->mode_ != mode) {
        mode = SHARED;
      }
      ++l;
    }
  }
  return mode;
}
