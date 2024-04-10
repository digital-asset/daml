// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.data.PeanoQueue.AssociatedValue

import scala.annotation.tailrec

/** A Peano priority queue is a mutable priority queue of key-value pairs with ordered keys,
  * starting from an index called the head,
  * where pairs may be added in any order,
  * but are polled strictly in their natural sequence.
  * The order on keys must be a linear sequence,
  * i.e., isomorphic to the order on a possibly unbounded interval of the integers.
  * If the priority queue is missing a key from the sequence, we cannot poll that key
  * until a key-value pair for that key is added.
  *
  * For example, in a priority queue with head 1, the keys polled are 1, then 2, then 3, etc.
  *
  * The head index is mutable, and increments each time the priority queue is successfully polled.
  * Keys are unique and their value associations may not be modified.
  *
  * @tparam K The type of keys
  * @tparam V The type of values
  */
trait PeanoQueue[K, V] {

  /** Returns the head of the [[PeanoQueue]].
    *
    * The head denotes the next key to be pulled.
    * The head key need not yet have been inserted into the [[PeanoQueue]].
    */
  def head: K

  /** Returns the front of the [[PeanoQueue]].
    *
    * The front is defined as the least key that is at least [[PeanoQueue.head]]
    * and that has not yet been inserted into the [[PeanoQueue]].
    */
  def front: K

  /** Inserts the key-value pair `(key, value)` to the [[PeanoQueue]].
    * This may change the value for [[PeanoQueue.front]]
    *
    * Inserting a key-value pair is idempotent.
    * If the keys are bounded from above, then the maximal value must not be inserted.
    *
    * @param key The key to be inserted.
    *            If the key has been added previously, then the following applies:
    *            <ul>
    *              <li>If the key is below [[PeanoQueue.head]], then this insert operation has no effect.</li>
    *              <li>If the key is at least [[PeanoQueue.head]] and the value is the same, this operation has no effect.</li>
    *              <li>If the key is at least [[PeanoQueue.head]] and the value is different,
    *                then the [[PeanoQueue]] is not changed and an [[java.lang.IllegalArgumentException]] is thrown.</li>
    *            </ul>
    * @param value The value associated with the key.
    * @return whether the key is at least [[PeanoQueue.head]]
    * @throws java.lang.IllegalArgumentException if the keys are bounded from above and `key` is the maximal value, or
    *                                            if the `key` is above [[PeanoQueue.head]] and has been added with a different value.
    */
  def insert(key: K, value: V): Boolean

  /** Whether the key `key` has already been `insert`-ed to the [[PeanoQueue]]. All values below the [[PeanoQueue.front]]
    * are considered to have been inserted to the [[PeanoQueue]], even if they are below the initial [[PeanoQueue.head]].
    */
  def alreadyInserted(key: K): Boolean

  /** Returns the value associated with the given `key`. */
  def get(key: K): AssociatedValue[V]

  /** Returns and drops the key at [[PeanoQueue.head]] and its associated value, if present.
    * If so, this also increments [[PeanoQueue.head]].
    * Returns [[scala.None]] to indicate that the key [[PeanoQueue.head]] has not yet been inserted.
    */
  def poll(): Option[(K, V)]

  /** Drops all elements from the [[PeanoQueue]] below the front and sets head to front.
    * @return [[scala.None$]] if the head already was at the front or the value associated with the last value before head otherwise.
    */
  def dropUntilFront(): Option[(K, V)] = {
    @tailrec
    def go(last: Option[(K, V)]): Option[(K, V)] = poll() match {
      case None => last
      case kvO @ Some(_) => go(kvO)
    }
    go(None)
  }
}

object PeanoQueue {

  sealed trait AssociatedValue[+V] extends Product with Serializable

  /** Returned by [[PeanoQueue.get]] for a `key` that is below [[PeanoQueue.head]]. */
  final case object BeforeHead extends AssociatedValue[Nothing]

  /** Returned by [[PeanoQueue.get]] for a `key` that is at least [[PeanoQueue.front]] and has not been inserted.
    * @param floor The value associated with the next smaller key.
    *              [[scala.None$]] if there is no such key that is at least [[PeanoQueue.head]]
    * @param ceiling The value associated with the next larger key, if any.
    */
  final case class NotInserted[V](floor: Option[V], ceiling: Option[V]) extends AssociatedValue[V]

  /** Returned by [[PeanoQueue.get]] for a `key` that is at least [[PeanoQueue.head]]
    * and has been inserted with the value [[value]]
    */
  final case class InsertedValue[V](value: V) extends AssociatedValue[V]
}
