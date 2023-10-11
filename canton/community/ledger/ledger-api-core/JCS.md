
# JCS

This document describes the JCS implementation used for ledger metering

## Background

As part of the ledger metering tamperproofing design a decision was made to use the JSON 
Canonicalization Scheme (JCS) to render a byte array that represented the contents of the
metering report.  The MAC of this byte array is then appended to the metering report JSON
as a tamperproofing measure.  The JCS spec is published 
as [RFC 8785](https://datatracker.ietf.org/doc/html/rfc8785).

## Java Implementation

The java reference implementation provided in the RFC is 
by [Samuel Erdtman](https://github.com/erdtman/java-json-canonicalization).  Concerns
about this implementation are that it:
* Has not been released since 2020
* Has only has one committer 
* Has vulnerability [CVE-2020-15250](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-15250)
* Does its own JSON parsing
* Has relatively few lines of testing given the size of the code base

## Javascript Implementation

Erdtman also has a [javascript implementation of the algorithm in 32 lines of javascript](https://github.com/erdtman/canonicalize/blob/master/lib/canonicalize.js).

The reason this implementation is so small is because:
* Javascript has native support for JSON
* The JCS spec uses the [javascript standard for number formatting](https://262.ecma-international.org/10.0/#sec-tostring-applied-to-the-number-type)
* The JCS uses JSON.stringify to serialize strings

## DA Implementation

Our starting point is similar to the situation in the javascript implementation in that:
* We have a parsed JSON object (or standard libraries we use to do this)
* We have JSON library functions that provide methods to stringify JSON objects.

For this reason the `Jcs.scala` class implements an algorithm similar to that in javascript.

## Number Limitation

When testing this implementation we discovered that the number formatting did not follow
the javascript standard. This is in part because scala implementations usually support
numeric values to be larger than a `IEEE-754` 64 bit `Double` (e.g. `BigDecimal`). 

### Workaround

By adding a limitation that we only support whole numbers smaller than 2^52 in absolute size
and formatting them without scientific notation (using `BigInt`) we avoid these problems.

This limitation is not restrictive for the ledger metering JSON whose only numeric field
is the event count.

### Approximation

The following approximates that javascript spec double formatting rules but was discarded
in favour of explicitly limiting the implementation.

```scala
  def serialize(bd: BigDecimal): String = {
    if (bd.isWhole && bd.toBigInt.toString().length < 22) {
      bd.toBigInt.toString()
    } else {
      val s = bd.toString()
      if (s.contains('E')) {
        s.replaceFirst("(\\.0|)E", "e")
      } else {
        s.replaceFirst("0+$", "")
      }
    }
  }
```

