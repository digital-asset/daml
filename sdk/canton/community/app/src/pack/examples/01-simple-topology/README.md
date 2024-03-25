# Simple Topology Example

The simple topology example features a simple setup, starting two participants named `participant1` 
and `participant2`, and a domain named `mydomain` in a single process.

How to run the example is featured in the [getting started tutorial](
https://docs.daml.com/canton/tutorials/getting_started.html#starting-canton).

The second file contains a set of Canton console commands that are run in order to connect the participants together
and test the connection.

The simple topology example can be invoked using

```
       ../../bin/canton -c simple-topology.conf --bootstrap simple-ping.canton 
```