// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package xbc

import scala.annotation.tailrec

// Examples writen natively in scala
object ByHandScala {

  def nfib(n: Long): Long = {
    // Not quite the fib function, as here we +1 in the recursive case.
    // So the function result is equal to the number of function calls.
    if (n < 2) {
      1
    } else {
      nfib(n - 1) + nfib(n - 2) + 1
    }
  }

  @tailrec
  def tripLoop(step: Long, acc: Long, i: Long): Long = {
    if (i < 1) acc
    else {
      tripLoop(step, acc + step, i - 1)
    }
  }

  def trip(i: Long): Long = {
    // compute 3*n using iteration
    val step = 3L
    tripLoop(step, 0L, i)
  }

}
