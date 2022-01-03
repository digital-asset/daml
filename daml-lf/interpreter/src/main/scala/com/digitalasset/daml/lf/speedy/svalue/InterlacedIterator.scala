// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy.svalue

// Assume the two iterators have the same size.
private[svalue] final class InterlacedIterator[X](iterLeft: Iterator[X], iterRight: Iterator[X])
    extends Iterator[X] {
  private[this] var left = true

  override def hasNext: Boolean = iterRight.hasNext

  override def next(): X =
    if (left) {
      left = false
      iterLeft.next()
    } else {
      left = true
      iterRight.next()
    }
}
