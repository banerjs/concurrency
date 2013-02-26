// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)

#include "txn/txn_processor.h"
#include <stdio.h>

#include <set>

#include "txn/lock_manager.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 100
#define QUEUE_COUNT 10

// Maximum number of transactions to deal with during validation and
// post-validation.
#define VALIDATION_MAX      1
#define POST_VALIDATION_MAX 1

// Quick define for being specific to a MODE
#define MODE_DEBUG P_OCC
#define MODE_PRINT(MSG) if (mode_ == MODE_DEBUG) { MSG; }

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT, QUEUE_COUNT), next_unique_id_(1) {
  MODE_PRINT(DERROR("Creating new Txn Processor. Mode = %d\n", mode))

  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING)
    lm_ = new LockManagerB(&ready_txns_);

  // Start 'RunScheduler()' running as a new task in its own thread.
  tp_.RunTask(
        new Method<TxnProcessor, void>(this, &TxnProcessor::RunScheduler));
}

TxnProcessor::~TxnProcessor() {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING)
    delete lm_;
}

void TxnProcessor::NewTxnRequest(Txn* txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult() {
  Txn* txn;
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  return txn;
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
    case SERIAL:                 RunSerialScheduler(); break;
    case LOCKING:                RunLockingScheduler(); break;
    case LOCKING_EXCLUSIVE_ONLY: RunLockingScheduler(); break;
    case OCC:                    RunOCCScheduler(); break;
    case P_OCC:                  RunOCCParallelScheduler(); break;
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn* txn;

  MODE_PRINT(DERROR("Running a Serial Scheduler\n"));

  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  Txn* txn;

  MODE_PRINT(DERROR("Running a Locking Scheduler\n"));

  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      int blocked = 0;
      // Request read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it))
          blocked++;
      }

      // Request write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        if (!lm_->WriteLock(txn, *it))
          blocked++;
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed.
      if (blocked == 0)
        ready_txns_.push_back(txn);
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Release read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Release write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        lm_->Release(txn, *it);
      }

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }

    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));
    }
  }
}

void TxnProcessor::RunOCCScheduler() {
  // CPSC 438/538:
  //
  // Implement this method! Note that implementing OCC may require
  // modifications to the Storage engine (and therefore to the 'ExecuteTxn'
  // method below).
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]

  Txn *txn;                             // Transaction pointer for current Txn

  MODE_PRINT(DERROR("Running an OCC Serial Scheduler\n"));

  while (tp_.Active()) {
    // Check for transactions waiting in the transaction queue
    if (txn_requests_.Pop(&txn)) {
      txn->occ_start_time_ = GetTime();  // Record a new Transaction
      MODE_PRINT(DERROR("New transaction %lu starting at %f\n", txn->unique_id_,
                        txn->occ_start_time_));

      // Start running the transaction in its own thread
      tp_.RunTask(new Method<TxnProcessor, void, Txn *>(
                    this,
                    &TxnProcessor::ExecuteTxn,
                    txn));
    }

    // Deal with transactions that have completed execution
    while (completed_txns_.Pop(&txn)) {
      bool valid = true;                // Boolean to keep track of txn validity

      MODE_PRINT(DERROR("Validating transaction %lu\n", txn->unique_id_));

      // If the transaction wants to abort, go ahead and let it abort. That
      // would save us significant overhead in checking
      if (txn->Status() == COMPLETED_A) {
        MODE_PRINT(DERROR("Transaction %lu is requesting an ABORT!\n",
                          txn->unique_id_));
        txn->status_ = ABORTED;
        txn_results_.Push(txn);
        continue;
      } else if (txn->Status() != COMPLETED_C) {
        // Invalid Txn Status!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Now we can be sure that the transaction wants to commit. Hence, go
      // ahead and validate this transaction's reads/writes
      for (map<Key, Value>::iterator it = txn->reads_.begin();
           it != txn->reads_.end(); ++it) {
        if (storage_.Timestamp(it->first) > txn->occ_start_time_)  // INVALID!!
          valid = false;
      }

      // If the transaction is valid, commit
      if (valid) {  // Could potentially check for Abort here
        MODE_PRINT(DERROR("Transaction %lu is valid!\n", txn->unique_id_));
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
        txn_results_.Push(txn);
      } else {  // Transaction is not valid, so roll it back
        MODE_PRINT(DERROR("Transaction %lu is invalid!\n", txn->unique_id_));
        (txn->reads_).clear();          // Remove all the reads done by Txn
        txn->status_ = INCOMPLETE;
        txn_requests_.Push(txn);        // Send Txn back to get re-evaluated
      }
    }
  }
}

void TxnProcessor::RunOCCParallelScheduler() {
  // CPSC 438/538:
  //
  // Implement this method! Note that implementing OCC with parallel
  // validation may require modifications to other files, most likely
  // txn_processor.h and possibly others.
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]

  Txn *txn;                             // Transaction pointer for current Txn

  MODE_PRINT(DERROR("Running an OCC Parallel Scheduler\n"));

  DIE("Parallel OCC is still being implemented");

  while (tp_.Active()) {
    int counter = 0;                    // Counter to keep track of number of
                                        // transactions that have been checked

    // Check for transactions waiting in the transaction queue
    if (txn_requests_.Pop(&txn)) {
      txn->occ_start_time_ = GetTime();  // Record a new Transaction
      MODE_PRINT(DERROR("New transaction %lu starting at %f\n", txn->unique_id_,
                        txn->occ_start_time_));

      // Start running the transaction in its own thread
      tp_.RunTask(new Method<TxnProcessor, void, Txn *>(
                    this,
                    &TxnProcessor::ExecuteTxn,
                    txn));
    }

    while (counter < VALIDATION_MAX &&   // Appropriate number of validations
           completed_txns_.Pop(&txn)) {  //  and there is a transaction waiting
      // If the transaction wants to abort, let it. Also check for errors
      if (txn->Status() == COMPLETED_A) {
        MODE_PRINT(DERROR("Transaction %lu is requesting an ABORT!\n",
                          txn->unique_id_));
        txn->status_ = ABORTED;
        txn_results_.Push(txn);
        continue;
      } else if (txn->Status() != COMPLETED_C) {
        // Invalid Txn Status!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // The transaction is set on committing. Send it for validation
      set<Txn*> txn_active_set = active_set_;  // Set the active set of this Txn
      active_set_.insert(txn);           // Insert this txn for others

      MODE_PRINT(DERROR("Sending transaction %lu for validation\n",
                        txn->unique_id_));

      // Validate the transaction in a separate thread
      tp_.RunTask(new Method<TxnProcessor, void, Txn *, set<Txn*> >(
                    this,
                    &TxnProcessor::ValidateTxn,
                    txn,
                    txn_active_set));
    }

    // Reset the counter in order to check post-validated
    counter = 0;
  }
}

void TxnProcessor::ExecuteTxn(Txn* txn) {
  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_.Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_.Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it) {
    storage_.Write(it->first, it->second);
  }

  // Set status to committed.
  txn->status_ = COMMITTED;
}

void TxnProcessor::ValidateTxn(Txn *txn, set<Txn*> active_set) {
}
