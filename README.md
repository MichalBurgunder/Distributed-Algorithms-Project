# Distributed-Algorithms-Project: Paxos
A basic paxos implementation written in python. 

Instructions on running:

## Simple run:

````
  ./run.sh ./fake-paxos 100
````

This will run paxos with no message loss. TYhe number indicates how many messages should be sent. 

## Loss run:
````
./run_loss.sh ./fake-paxos 100
````

This will paxos with some message loss. The loss is given in the file (0.1). 

## Catch-up run:
````
./run_catch_up.sh ./fake-paxos 100
````

This will run paxos with a catch-up, i.e. time delay.

You can choose how many acceptors/proposers you would like by specifying this in the appropriate file, by either commenting our certain lines, or adding new ones.