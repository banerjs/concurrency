#include "txn/storage.h"

bool Storage::Read(Key key, Value* result) {
  if (data_.count(key)) {
    *result = data_[key];
    return true;
  } else {
    return false;
  }
}

void Storage::Write(Key key, Value value) {
  data_[key] = value;
  timestamps_[key] = GetTime();
}

double Storage::Timestamp(Key key) {
  if (timestamps_.count(key) == 0)
    return 0;
  return timestamps_[key];
}

bool MVStorage::Read(Key key, Value* result, uint64 mvcc_txn_id,
                     const map<uint64, TxnStatus>& pg_log_snapshot) {
  // CPSC 438/538:
  //
  // Implement this method!
  // [Returning false for now so that we at least compile.]
  return false;
}

void MVStorage::Write(Key key, Value value, uint64 mvcc_txn_id,
                      const map<uint64, TxnStatus>& pg_log_snapshot) {
  // CPSC 438/538:
  //
  // Implement this method!
}

