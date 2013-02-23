// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "txn/txn_processor.h"

#include <vector>

#include "txn/txn_types.h"
#include "utils/testing.h"

// Returns a human-readable string naming of the providing mode.
string ModeToString(CCMode mode) {
  switch (mode) {
    case SERIAL:                 return " Serial   ";
    case LOCKING_EXCLUSIVE_ONLY: return " Locking A";
    case LOCKING:                return " Locking B";
    case OCC:                    return " OCC      ";
    case MVCC:                   return " MVCC     ";
    default:                     return "INVALID MODE";
  }
}

class LoadGen {
 public:
  virtual ~LoadGen() {}
  virtual Txn* NewTxn() = 0;
};

class RMWLoadGen : public LoadGen {
 public:
  RMWLoadGen(int dbsize, int rsetsize, int wsetsize, double wait_time)
    : dbsize_(dbsize),
      rsetsize_(rsetsize),
      wsetsize_(wsetsize),
      wait_time_(wait_time) {
  }

  virtual Txn* NewTxn() {
    return new RMW(dbsize_, rsetsize_, wsetsize_, wait_time_);
  }

 private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  double wait_time_;
};

class RMWLoadGen2 : public LoadGen {
 public:
  RMWLoadGen2(int dbsize, int rsetsize, int wsetsize, double wait_time)
    : dbsize_(dbsize),
      rsetsize_(rsetsize),
      wsetsize_(wsetsize),
      wait_time_(wait_time) {
  }

  virtual Txn* NewTxn() {
    // 10% of transactions are READ only transactions and run for the full
    // transaction duration. The rest are very fast (< 0.1ms), high-contention
    // (65%+) updates.
    if (rand() % 100 < 10)
      return new RMW(dbsize_, rsetsize_, 0, wait_time_);
    else
      return new RMW(dbsize_, 0, wsetsize_, 0);
  }

 private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  double wait_time_;
};

void Benchmark(const vector<LoadGen*>& lg) {
  // Number of transaction requests that can be active at any given time.
  int active_txns = 100;

  // Set initial db state.
  map<Key, Value> db_init;
  for (int i = 0; i < 10000; i++)
    db_init[i] = 0;

  // For each MODE...
  for (CCMode mode = SERIAL; mode <= MVCC; mode = static_cast<CCMode>(mode+1)) {
    // Print out mode name.
    cout << ModeToString(mode) << flush;

    // For each experiment...
    for (uint32 exp = 0; exp < lg.size(); exp++) {
      int txn_count = 0;

      // Create TxnProcessor in next mode.
      TxnProcessor* p = new TxnProcessor(mode);

      // Initialize data with initial db state.
      Put init_txn(db_init);
      p->NewTxnRequest(&init_txn);
      p->GetTxnResult();

      // Record start time.
      double start = GetTime();

      // Start specified number of txns running.
      for (int i = 0; i < active_txns; i++)
        p->NewTxnRequest(lg[exp]->NewTxn());

      // Keep 100 active txns at all times for the first full second.
      while (GetTime() < start + 1) {
        delete p->GetTxnResult();
        txn_count++;
        p->NewTxnRequest(lg[exp]->NewTxn());
      }

      // Wait for all of them to finish.
      for (int i = 0; i < active_txns; i++) {
        delete p->GetTxnResult();
        txn_count++;
      }

      // Record end time.
      double end = GetTime();

      // Print throughput
      cout << "\t" << (txn_count / (end-start)) << "\t" << flush;

      // Delete TxnProcessor.
      delete p;
    }

    cout << endl;
  }
}

int main(int argc, char** argv) {
  cout << "\t\t\t    Average Transaction Duration" << endl;
  cout << "\t\t0.1ms\t\t1ms\t\t10ms\t\t100ms";
  cout << endl;

  vector<LoadGen*> lg;

  cout << "Read only" << endl;
  lg.push_back(new RMWLoadGen(10000, 10, 0, 0.0001));
  lg.push_back(new RMWLoadGen(10000, 10, 0, 0.001));
  lg.push_back(new RMWLoadGen(10000, 10, 0, 0.01));
  lg.push_back(new RMWLoadGen(10000, 10, 0, 0.1));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "1% contention" << endl;
  lg.push_back(new RMWLoadGen(10000, 10, 10, 0.0001));
  lg.push_back(new RMWLoadGen(10000, 10, 10, 0.001));
  lg.push_back(new RMWLoadGen(10000, 10, 10, 0.01));
  lg.push_back(new RMWLoadGen(10000, 10, 10, 0.1));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "10% contention" << endl;
  lg.push_back(new RMWLoadGen(1000, 10, 10, 0.0001));
  lg.push_back(new RMWLoadGen(1000, 10, 10, 0.001));
  lg.push_back(new RMWLoadGen(1000, 10, 10, 0.01));
  lg.push_back(new RMWLoadGen(1000, 10, 10, 0.1));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "65% contention" << endl;
  lg.push_back(new RMWLoadGen(100, 10, 10, 0.0001));
  lg.push_back(new RMWLoadGen(100, 10, 10, 0.001));
  lg.push_back(new RMWLoadGen(100, 10, 10, 0.01));
  lg.push_back(new RMWLoadGen(100, 10, 10, 0.1));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "100% contention" << endl;
  lg.push_back(new RMWLoadGen(10, 0, 10, 0.0001));
  lg.push_back(new RMWLoadGen(10, 0, 10, 0.001));
  lg.push_back(new RMWLoadGen(10, 0, 10, 0.01));
  lg.push_back(new RMWLoadGen(10, 0, 10, 0.1));


  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "High contention mixed read/write" << endl;
  lg.push_back(new RMWLoadGen2(100, 20, 10, 0.0001));
  lg.push_back(new RMWLoadGen2(100, 20, 10, 0.001));
  lg.push_back(new RMWLoadGen2(100, 20, 10, 0.01));
  lg.push_back(new RMWLoadGen2(100, 20, 10, 0.1));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();
}

