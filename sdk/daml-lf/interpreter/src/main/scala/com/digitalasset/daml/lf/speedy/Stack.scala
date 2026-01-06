// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

// Ad-hoc data structure backed by an array to implement speedy stacks.
// This implementation is tailored specifically for this use case and is not intended to be a general-purpose stack.
// Key characteristics:
//  - No bounds checking is performed.
//  - It is 1-indexed, meaning the top of the stack is at index 1.
//  - The `push` method assumes the stack is not full; growing the stack when needed is the caller's responsibility.
//  - Includes a `keep` operation to retain the oldest `n` elements of the stack while discarding the rest.
//
// We force `X` to be an AnyRef to avoid the need a ClassTag.
private[speedy] final class Stack[X <: AnyRef](initialCapacity: Int) extends Iterable[X] {

  assert(0 < initialCapacity)

  private[this] var array: Array[AnyRef] = Array.ofDim[AnyRef](initialCapacity)
  private[this] var size_ : Int = 0

  def capacity: Int = array.length

  override def size: Int = size_

  // Double the capacity of the stack
  def grow(): Unit = {
    val oldCapacity = capacity
    val oldArray = array
    array = Array.ofDim(oldCapacity * 2)
    System.arraycopy(oldArray, 0, array, 0, oldCapacity)
  }

  def isFull: Boolean =
    size == capacity

  // It is the responsibility of the caller to check the stack is not full
  def push(value: X): Unit = {
    array(size_) = value
    size_ += 1
  }

  def pop: X = {
    size_ -= 1
    val x = array(size)
    array(size) = null // drop the reference
    x.asInstanceOf[X]
  }

  // keep the n oldest elements of the stack
  def keep(n: Int): Unit = {
    var i = n
    while (i < size) {
      array(i) = null // drop the references
      i += 1
    }
    size_ = n
  }

  // Indexed from 1 by relative offset from the top of the stack (1 is top!)
  @throws[ArrayIndexOutOfBoundsException]
  def apply(i: Int): X =
    array(size_ - i).asInstanceOf[X]

  override def iterator: Iterator[X] =
    array.iterator.take(size).asInstanceOf[Iterator[X]]

  override def knownSize: Int = size

}
