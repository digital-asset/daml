# proto3-wire

[![Build Status](https://travis-ci.org/awakesecurity/proto3-wire.svg?branch=master)](https://travis-ci.org/awakesecurity/proto3-wire)

This library provides a low-level implementation of the [Protocol Buffers version 3 wire format](https://developers.google.com/protocol-buffers/docs/encoding).

This library takes a minimalist approach, supporting only the basic wire format.
In particular, correctness is left up to the user in many places (for example,
ensuring that encoders use increasing field numbers).

There are approaches which can give more guarantees, such as Generics and Template
Haskell, but those approaches are not implemented here and are left to
higher-level libraries.

## Building

Install [Stack](http://docs.haskellstack.org/en/stable/README/), clone this repository, and run:

```text
stack build
```

To run tests or generate documentation, use

```text
stack build [--test] [--haddock]
```
