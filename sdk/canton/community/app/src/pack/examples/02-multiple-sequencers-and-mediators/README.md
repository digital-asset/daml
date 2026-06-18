# Topology example with multiple Sequencers and Mediators

The topology example features two Sequencers and two Mediators; it also starts two Participants named `participant1`
and `participant2` and the Synchronizer named `da` in a single process.

How to run the example is featured in the [getting started tutorial](
https://docs.daml.com/canton/tutorials/getting_started.html#starting-canton).

The second file contains a set of Canton console commands used to connect the Participants and test the connection.

This topology example can be bootstrapped using

```
       ../../bin/canton -c multiple-sequencers-and-mediators.conf --bootstrap simple-ping.canton
```
