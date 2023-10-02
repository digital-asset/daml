// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package data

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer

object TreeMap {

  // Linear time builder for immutable TreeMap.
  // Requires the entries to be ordered, crashes if not.
  // Like Normal TreeMap builder, in case of duplicate key, the last wins
  // Relies on the fact that immutable.TreeMap.from is linear when its argument
  // is a SortedMap with the same ordering.
  @throws[IllegalArgumentException]
  def fromOrderedEntries[K, V](
      entries: Iterable[(K, V)]
  )(implicit order: Ordering[K]): immutable.TreeMap[K, V] =
    if (entries.isEmpty)
      immutable.TreeMap.empty
    else {

      val buffer = new ArrayBuffer[(K, V)]()
      buffer.sizeHint(entries.knownSize)
      val it = entries.iterator
      var previous = it.next()

      it.foreach { next =>
        order.compare(previous._1, next._1).sign match {
          case -1 =>
            val _ = buffer.addOne(previous)
          case 0 =>
          case _ =>
            throw new IllegalArgumentException("the entries are not ordered")
        }
        previous = next
      }
      val _ = buffer.addOne(previous)

      val fakeSortedMap = new immutable.SortedMap[K, V] {
        override def ordering: Ordering[K] = order
        override def iterator: Iterator[(K, V)] = buffer.iterator
        override val size: Int = buffer.size

        // As for scala 2.12.10 immutable.TreeMap.from does not use the following methods
        override def removed(key: K): Nothing = ???
        override def updated[V1 >: V](key: K, value: V1): Nothing = ???
        override def iteratorFrom(start: K): Nothing = ???
        override def keysIteratorFrom(start: K): Nothing = ???
        override def rangeImpl(from: Option[K], until: Option[K]): Nothing = ???
        override def get(key: K): Nothing = ???
      }

      immutable.TreeMap.from(fakeSortedMap)
    }
}
