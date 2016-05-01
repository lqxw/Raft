// This is a C++ implementation to simulate the raft protocol by
// creating multiples instances and use multi-thread to achieve concurrency.

// Compile command: g++ -std=c++11 -pthread raft.cc -o a
// Run command: ./a [-o] > log.txt

// -o means use optimized version. The steps will be printed to stand out
// and the final result will be printed to stand error

#ifndef RAFT_H
#define RAFT_H

#include <iostream>
#include <algorithm>
#include <ctime>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <vector>
#include <map>
#include <string>
#include <unistd.h>
#include <cstdlib>
#include <cassert>

using namespace std;

struct LogEntry {
  int term;
  string text;
  LogEntry(int t, string s) {
    this->term = t;
    text = s;
  }
  string to_string() {
    string ret = std::to_string(term) + "-" + text;
    return ret;
  }
};

void crashRestartMachine(int crashId);

int makeTimeout();

int getCommitIndex(vector<int> matchIndex);

void printLog(const vector<LogEntry>& log);

// remove include start to end
void removeLog(vector<LogEntry>& log, int start);

// include start and end
vector<LogEntry> getSubVector(const vector<LogEntry>& log, int start);

void millSleep(int x);

long long int getCurrentTime();

void crashRestartMachine(int crashId);

void crashMachines();

void finish();

void makeLog();

class Node {
public:
  atomic<int> commitIndex;
  atomic<int> leaderCommit;
  atomic<int> lastApplied;
  vector<int> matchIndex;
  vector<int> nextIndex;

  int id;
  volatile bool isCrash;
  volatile int status; // 1:leader, 2:follower, 3:candidate
  std::mutex status_mx;
  volatile int term; // ret 0 means success, otherwise means update term
  vector<LogEntry> log;
  std::mutex log_mx;
  volatile long long int lastUpdateTime; // timer
  atomic<int> votedTerm;
  thread* t1; // for electTimer
  thread* t2; // for heart-beat
  atomic<bool> t1Start; // flag for t1 status

  void deleteT1();

  void deleteT2();

  void crash();

  void start();

  // run in thread t1, track head-beat time out
  void electTimer();

  bool startElect();

  void restartT1();

  void restartT2();

  void addLog(string s); // rpc

  int sendLog(); // rpc

  void leaderHeardBeat();

  // 0 means success, other int means update term, -1 means fail, -2 means crash
  int appendEntries(vector<LogEntry> leaderLog, int leaderTerm, int leaderId,
                    int prevLogTerm, int prevLogIndex, int leaderCommit); // rpc

  int requestVote(int candidateTerm, int candidateId, int candidateLogSize, vector<int>& result); // rpc
  
  Node(int idNumber);
};

#endif
