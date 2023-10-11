# Composability Example

The composability example features a workflow that spans across two domains.
It starts two domains and five participants in a single process.
The details are described in the [composability tutorial](
https://docs.daml.com/canton/tutorials/composability.html).

The composability examples can be invoked from the root directory of the Canton release using

```
       ./bin/canton -c examples/05-composability/composability.conf --bootstrap examples/05-composability/composability1.canton
       ./bin/canton -c examples/05-composability/composability.conf --bootstrap examples/05-composability/composability2.canton
```

It can be run from other directories if the path to the CantonExamples.dar file in the examples folder
is set as the system property canton-examples.dar-path:

```
       ./bin/canton -Dcanton-examples.dar-path=<path-to-dar-file> -c examples/05-composability/composability.conf --bootstrap examples/05-composability/composability1.canton
```