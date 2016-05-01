# Raft
A stand alone raft implementation runs on local.

The code is to compare the performance bewteen naive Raft and optimized Raft.

Compile command: g++ -std=c++11 -pthread raft.cc -o a

Run command: ./a [-o] > log.txt

-o means use optimized version. The internal steps will be printed to stand out and the final result will be printed to stand error.
