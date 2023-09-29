// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package data

import scala.collection.immutable

object TreeMap {

  import Ordering.Implicits._

  // Linear time builder for immutable TreeMap.
  // Requires the entries to be strictly ordered, crashes if not.
  // Relies on the fact that immutable.TreeMap.from is linear when its argument
  // is a SortedMap with the same ordering.
  @throws[IllegalArgumentException]
  def fromOrderedEntries[K: Ordering, V](entries: Iterable[(K, V)]): immutable.TreeMap[K, V] =
    if (entries.isEmpty)
      immutable.TreeMap.empty
    else {

      val _ = entries.tail.foldLeft(entries.head._1) { case (previousKey, (k, _)) =>
        require(previousKey < k, "the entries are not strictly ordered")
        k
      }

      val fakeSortedMap = new immutable.SortedMap[K, V] {
        override def ordering: Ordering[K] = implicitly
        override val iterator: Iterator[(K, V)] = entries.iterator
        override def size: Int = entries.size

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
