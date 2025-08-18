// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

// Adhoc data structure back by an array to implement speedy stacks.
// It has been tailored for this usage and are not design to be a general-purpose stack.
// In particular:
//  - it is 1-indexed, i.e. the top of the stack is at index 1.
//  - push returns the number of elements added to the underlying array, everytime it grows.
//  - it implements an operation `keep` to keep the oldest n elements of the stack, dropping the rest.
//
// We force `X` to be an AnyRef to avoid the need a ClassTag.
private[speedy] class Stack[X <: AnyRef](initialCapacity: Int) extends Iterable[X] {

  assert(0 < initialCapacity)

  private[this] var array: Array[AnyRef] = Array.ofDim[AnyRef](initialCapacity)
  private[this] var size_ : Int = 0

  def capacity: Int = array.length

  override def size: Int = size_

  private[this] def grow: Int = {
    val capacity = this.capacity
    val oldArray = array
    array = Array.ofDim(capacity * 2)
    Array.copy(oldArray, 0, array, 0, capacity)
    capacity
  }

  // Return the number of elements added to the underlying array
  def push(value: X): Int = {
    val increase = if (size < capacity) 0 else grow
    array(size_) = value
    size_ += 1
    increase
  }

  def pop: X = {
    size_ -= 1
    val x = array(size)
    array(size) = null // drop the reference
    x.asInstanceOf[X]
  }

  // keep the n oldest elements of the stack
  def keep(n: Int): Unit = {
    if (n < 0 || size < n)
      throw new IllegalArgumentException(s"Invalid stack index: $n, size: $size")
    var i = n
    while (i < size) {
      array(i) = null // drop the references
      i += 1
    }
    size_ = n
  }

  // Variables which reside on the stack. Indexed (from 1) by relative offset from the top of the stack (1 is top!)
  def apply(i: Int): X = {
    if (i <= 0 || size < i)
      throw new IllegalArgumentException(s"Invalid stack index: $i, size: $size")
    array(size_ - i).asInstanceOf[X]
  }

  override def iterator: Iterator[X] = array.iterator.take(size).asInstanceOf[Iterator[X]]

  override def knownSize: Int = size

}
