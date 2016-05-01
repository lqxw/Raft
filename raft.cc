#include "raft.h"

// global config
map<int, Node*> idLookupTable; // id to node map
const int electTimeOut = 300; // all time are in millisecond
const int crashRestartTime = 1000;
const int MaxMachine = 10; // do not exceed 20 if run on single machine
const int logNumber = 150;
const int heartBeatTime = 10;
const int clientSendLogTime = 300;
const int maxFailureNumber = MaxMachine/2 - 1;
const int failureProbability = 20; // 1/20 failure rate when send log
// global config ends

bool useOptimize = false;
int crashNumber = 0;
int currentLeaderId = -1;
int crashTimes = 0;
int leaderCrashTimes = 0;

int makeTimeout() {
  return electTimeOut+rand()%electTimeOut;
}

int getCommitIndex(vector<int> matchIndex) {
  std::sort(matchIndex.begin(), matchIndex.end(), std::greater<int>());
  int position  = matchIndex.size()/2;
  return matchIndex[position];
}

void printLog(const vector<LogEntry>& log) {
  for(auto e: log) {
    cout<<e.to_string()<<endl;
  }
  cout<<endl;
}

void removeLog(vector<LogEntry>& log, int start) {
  assert(start >= 0);
  log.erase(log.begin() + start, log.end());
}

vector<LogEntry> getSubVector(const vector<LogEntry>& log, int start) {
  assert( start >= 0);
  if (start >= log.size()) {
    return vector<LogEntry> ();
  }
  auto a = log.begin() + start;
  auto b = log.end();
  vector<LogEntry> ret(a,b);
  return ret;
}

void millSleep(int x) {
  std::this_thread::sleep_for(std::chrono::milliseconds(x));
}

long long int getCurrentTime() {
  long long ret =
    std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
  return ret;
}


void Node::deleteT1() {
  if (t1) {
    t1->join();
    delete t1;
    t1 = NULL;
  }
}

void Node::deleteT2() {
  if (t2) {
    t2->join();
    delete t2;
    t2 = NULL;
  }
}

void Node::start() {
  isCrash = false;
  lastUpdateTime = getCurrentTime(); // reset timer
  if (status != 1) { // make to follower
    restartT1();
  }
  else { //leader
    restartT2();
  }
}

void Node::electTimer() { // run in thread t1, track head-beat time out
  while(1) {
    if (isCrash) {
      break;
    }
    if (status == 1) break; // leader do not have timer
    int timeOutValue = makeTimeout();
    if ((getCurrentTime() - lastUpdateTime) > timeOutValue) {
      if (startElect()) break;
      millSleep(rand()%1000);
    }
  }
  t1Start = false;
}

bool Node::startElect() {
  if (isCrash) return false;
  long long int startElectTime = getCurrentTime();
  term++;
  status = 3;
  vector<int> result;
  for(int i = 0; i < MaxMachine; i++) result.push_back(0);

  for(auto i: idLookupTable) {
    int voteId = i.first;
    log_mx.lock();
    int voteTerm = idLookupTable[voteId]->requestVote(term, id, log.size(), result);
    log_mx.unlock();
    if (voteTerm > term) { // convert to follower state
      status = 2;
      return false;
    }
  }

  bool success = true; 
  int acc = 0;
  for(int i: result) {
    if (i == 1) acc++;
    if (i == -1) success = false;
  }

  if (success) success = (acc > ((int)result.size()/2));
  if (!success) return false;

  // received rpc during elect
  if (lastUpdateTime > startElectTime) {
    cout<<"Received rpc during elect! id is "<<id<<endl;
    return false;
  }

  status = 1;
  currentLeaderId = id;
  leaderCommit = (int)commitIndex;
  for(int i = 0; i < MaxMachine; i++) {
    nextIndex[i] = log.size();
  }
  cout<<"machine "<<id<<" becomes the leader"<<endl;
  sendLog();
  restartT2();
  return true;
}

void Node::restartT1() {
  if (t1Start) return;
  t1Start = true;
  deleteT1();
  t1 = new thread(&Node::electTimer, this);
}

void Node::restartT2() {
  deleteT2();
  t2 = new thread(&Node::leaderHeardBeat, this);
}

void Node::addLog(string s) { // rpc
  if (isCrash) {
    cout<<"log "<<s<<" discarded"<<endl;
    return; // leader crash, log entry discard
  }
  cout<<"add "<<s<<" to "<<id<<endl;
  log_mx.lock();
  log.push_back(LogEntry(term, s));
  log_mx.unlock();
}

int Node::sendLog() { // rpc
  std::lock_guard<std::mutex> lock(status_mx);
  if (isCrash) return -2;
  std::lock_guard<std::mutex> log_lock(log_mx);
  if (status != 1) return 0; // add
  for(int i = 0; i < MaxMachine; i++) {
    if (isCrash) {
      cout<<"Crash machine "<<id<<" during sendLog begin, i is "<<i<<endl;
      return -2;
    }
    if (i == id) continue; // this
    if (rand()%failureProbability == 0 &&
        crashNumber <= maxFailureNumber-1 &&
        (int)log.size()-1 >= nextIndex[i] &&
        idLookupTable[i]->isCrash == false) { // non-empty
      cout<<"Crash machine "<<id<<" during sendLog, i is "<<i<<endl;
      crashRestartMachine(id); // random crash during log send
      return -2;
    }
    while(1) {
      int prevLogIndex = nextIndex[i] - 1;
      int prevLogTerm;
      if (prevLogIndex == -1)
        prevLogTerm = -1;
      else
        prevLogTerm = log[prevLogIndex].term;

      vector<LogEntry> v = getSubVector(log, nextIndex[i]);

      int result = idLookupTable[i]->appendEntries(v, term, id, prevLogTerm, prevLogIndex, leaderCommit);

      if (result == 0) { // success
        matchIndex[i] =  (int)log.size() - 1;
        nextIndex[i] = (int)log.size();
        break;
      }

      if (result == -2) break; // machine crash
      
      if (result > 0) { // expired leader
        return result;
      }
      if (result == -1) {
        cout<<"Log not match"<<endl;
        cout<<"id, leader, prevLogTerm, prevLogIndex are "<<i<<" "<<id<<" "<<prevLogTerm<<" "<<prevLogIndex<<endl;
        nextIndex[i] = nextIndex[i] - 1;
        continue;
      }
    }
  }
  leaderCommit = max((int)leaderCommit, getCommitIndex(matchIndex)); // if leaderCommit is new, do not override 
  commitIndex = (int)leaderCommit;
  matchIndex[id] = (int)log.size()-1;
  return 0;
}

void Node::leaderHeardBeat() {
  while(1) {
    if (isCrash) {
      return;
    }
    if (status != 1) return; // convert to follower
    int result = sendLog();
    if (result > 0) { // expired leader
      term = result;
      status = 2;
      restartT1();
      cout<<"expired leader "<<id<<" convert to follower"<<endl;
      return;
    }
    millSleep(heartBeatTime);
  }
}

// rpc
// 0 means success, other int means update term, -1 means fail, -2 means crash
int Node::appendEntries(vector<LogEntry> leaderLog, int leaderTerm, int leaderId, int prevLogTerm, int prevLogIndex, int leaderCommit) {
  if (isCrash) {
    return -2;
  }
  if (leaderTerm == term) {
    assert(status != 1); // can not have two leader in a same term
    lastUpdateTime = getCurrentTime();
  }
  else if (leaderTerm < term){
    cout<<"called expired appendEntries by "<<leaderId<<" on id "<<id<<endl;
    return term; // called by expired leader
  }
  else {
    lastUpdateTime = getCurrentTime();
    if (status == 1) { // itself is expired leader
      status = 2;
      deleteT2();
      restartT1();
      cout<<"In time "<<getCurrentTime()<<" machine "<<id<<"convert to follower"<<endl;
    }
    if (status == 3) status = 2;
    term = leaderTerm;
  }

  if (leaderLog.size() == 0)
  	cout<<"In time "<<getCurrentTime()<<" Empty heart-beat on machine "<<id<<" called appendEntries (leaderTerm "<<leaderTerm<<", leaderId "
                                        <<leaderId<<", prevLogTerm "
                                        <<prevLogTerm<<", prevLogIndex "
                                        <<prevLogIndex<<", leaderCommit "
                                        <<leaderCommit<<")"<<endl;
  else
  	cout<<"On machine "<<id<<" called appendEntries (leaderTerm "<<leaderTerm<<", leaderId "
                       <<leaderId<<", prevLogTerm "
                       <<prevLogTerm<<", prevLogIndex "
                       <<prevLogIndex<<", leaderCommit "
                       <<leaderCommit<<")"<<endl;
  if (prevLogIndex == -1) { // first time
    assert(prevLogTerm == -1);
    log = leaderLog;
    return 0;
  }
  else if ( (int)log.size() < prevLogIndex+1 ||
            log[prevLogIndex].term != prevLogTerm) {
  	cout<<"can't find log"<<endl;
    return -1; // not match
  }
  else {
    if (log.size() != prevLogIndex+1) { // take whatever in leaderLog and put in to local log
      cout<<"override previous log on machine "<<id<<"!"<<endl;
      cout<<"overrided log are "<<endl;
      printLog(getSubVector(log, prevLogIndex+1));
      removeLog(log, prevLogIndex+1);
    }
    log.insert(log.end(), leaderLog.begin(), leaderLog.end());
  }

  if (leaderCommit > commitIndex) {
    commitIndex = min(leaderCommit, (int)log.size()-1);
  }
  return 0;
}

// rpc
int Node::requestVote(int candidateTerm, int candidateId, int candidateLogSize, vector<int>& result) {
  std::lock_guard<std::mutex> lock(status_mx);
  if (isCrash) return -1;
  if (candidateTerm > term) {  // others have larger term, convert to follower
    if (status == 1) { // itself is expired leader
      currentLeaderId = -1;
      status = 2;
      deleteT2();
      restartT1();
      cout<<"Machine "<<id<<" convert to follower!"<<endl;
    }
  	term = candidateTerm;
  	status = 2;
  	lastUpdateTime = getCurrentTime();
  }
  cout<<"In time "<<getCurrentTime()<<" on machine "<<id<<" called requestVote term "<<candidateTerm<<" by "<<candidateId<<" ";
  if (candidateLogSize < log.size()) {
    if (useOptimize) result[id] = -1;
    else result[id] = 0;
    cout<<"result -1"<<endl;
    return 0;
  }
  if (votedTerm >= candidateTerm) {
    result[id] = 0;
    cout<<"result false"<<endl;
    return 0;
  }
  if (candidateTerm >= term && candidateLogSize >= log.size()) {
    result[id] = 1;
    votedTerm = candidateTerm;
    cout<<"result true"<<endl;
    if (candidateId != id) // not itself
    	lastUpdateTime = getCurrentTime(); // reset timer
    return 0;
  }
  result[id] = 0;
  if (candidateTerm < term) return term;
  cout<<"result false"<<endl;
  return -1; // log is less up-to-date
}

Node::Node(int idNumber) {
  commitIndex = -1;
  lastApplied = -1;
  leaderCommit = -1;
  for(int i = 0; i < MaxMachine; i++) {
    matchIndex.push_back(-1);
  }
  for(int i = 0; i < MaxMachine; i++) {
    nextIndex.push_back(0);
  }

  isCrash = false;
  status = 3;
  term = 0;
  votedTerm = -1;
  t1 = NULL;
  t2 = NULL;
  id = idNumber;
  idLookupTable[idNumber] = this;
}

vector<thread*> crashThreadPool;

void crashRestartMachine(int crashId) {
  if (!idLookupTable[crashId]->isCrash) {
    idLookupTable[crashId]->isCrash = true;
    if (currentLeaderId == crashId) {
      currentLeaderId = -1;
      leaderCrashTimes++;
    }
    crashNumber++;
    crashTimes++;
  }
  thread* makeStart = new std::thread([crashId](){
    millSleep(crashRestartTime);
    cout<<"restart machine "<<crashId<<endl;
    idLookupTable[crashId]->start();
    crashNumber--;
  });
  crashThreadPool.push_back(makeStart);
}

void crashMachines() {
  while(1) {
    millSleep(crashRestartTime);
    int crashId = rand()%MaxMachine; // currentLeaderId; // rand()%10;

    if (idLookupTable[crashId]->isCrash) continue;
    if (crashNumber >= maxFailureNumber) continue;
    cout<<"Crash machine "<<crashId<<endl;
    crashRestartMachine(crashId);
  }
}

void finish() {
  for(int i = 0; i < MaxMachine; i++) {
    cerr<<"In node "<<i<<", "
        <<"commitIndex is "<<idLookupTable[i]->commitIndex<<", "
        <<"logsize is "<<idLookupTable[i]->log.size()<<endl;
    // printLog(idLookupTable[i]->log);
  }
  int maxTerm = 0;
  for(int i = 0; i < MaxMachine; i++) {
    maxTerm = max(maxTerm, (int)idLookupTable[i]->term);
  }
  cerr<<endl<<"Failure number is "<<crashTimes<<endl;
  cerr<<"Leader failure number is "<<leaderCrashTimes<<endl;
  cerr<<"Max term is "<<maxTerm<<endl;
  cerr<<"program finishes"<<endl;
  exit(0);
}

void makeLog() {
  millSleep(crashRestartTime);
  vector<string> logSend;
  for(int i = 1; i <= logNumber; i++) {
    logSend.push_back(string("log_")+to_string(i));
  }
  while(logSend.size() != 0) {
    millSleep(clientSendLogTime);
    string logToSend = logSend[0];
    if (currentLeaderId == -1) continue;
    if (idLookupTable[currentLeaderId]->isCrash) continue;
    assert(idLookupTable[currentLeaderId]->status == 1);
    idLookupTable[currentLeaderId]->addLog(logToSend);
    logSend.erase(logSend.begin());
  }
  millSleep(2*crashRestartTime); // leave time for machines to restart and reach consistency
  finish();
}

int main(int argc, char* argv[]) {
  if (argc >= 2 && string(argv[1]) == "-o") {
    cerr << "use optimize"<<endl;
    useOptimize = true;
  }
  cerr<<"Program is running, it may take a few minutes"<<endl;
  srand(time(NULL));
  for(int i = 0; i < MaxMachine; i++) {
    Node* node = new Node(i);
  }
  for(int i = 0; i < MaxMachine; i++) {
    idLookupTable[i]->start();
  }
  thread logThread(makeLog);
  thread crash(crashMachines);
  logThread.join();
}
