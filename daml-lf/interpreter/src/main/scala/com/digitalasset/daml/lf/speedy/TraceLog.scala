// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.data.Ref.Location

final case class TraceLog(capacity: Int) {

  private val buffer = Array.ofDim[(String, Option[Location])](capacity)
  private var pos: Int = 0
  private var size: Int = 0

  def add(message: String, optLocation: Option[Location]): Unit = {
    buffer(pos) = (message, optLocation)
    pos = (pos + 1) % capacity
    if (size < capacity)
      size += 1
  }

  def iterator: Iterator[(String, Option[Location])] =
    new RingIterator(if (size < capacity) 0 else pos, size, buffer)
}

private final class RingIterator[A](ringStart: Int, ringSize: Int, buffer: Array[A])
    extends Iterator[A] {
  private var pos: Int = ringStart
  private var first = true
  private def nextPos: Int = (pos + 1) % ringSize
  def hasNext: Boolean = ringSize != 0 && (first || pos != ringStart)
  def next: A = {
    val x = buffer(pos)
    first = false
    pos = nextPos
    x
  }
}
