// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import scala.collection.IndexedSeqView
import scala.collection.immutable.{SortedMap, TreeMap}

object TreeMapFactory {

  private[TreeMapFactory] final class SortedListAdapter[K, V](
      entries: IndexedSeqView[(K, V)],
      val ordering: Ordering[K],
  ) extends SortedMap[K, V] {
    override def updated[V1 >: V](key: K, value: V1): Nothing = ???
    override def iteratorFrom(start: K): Nothing = ???
    override def keysIteratorFrom(start: K): Nothing = ???
    override def removed(key: K): Nothing = ???
    override def get(key: K): Nothing = ???
    override def rangeImpl(from: Option[K], until: Option[K]): Nothing = ???

    override def iterator: Iterator[(K, V)] = {
      var lastKey = Option.empty[K]
      entries.iterator.map { case entry @ (k, _) =>
        require(lastKey.forall(ordering.lteq(_, k)), "The entries are not ordered")
        lastKey = Some(k)
        entry
      }
    }
    override def size: Int = entries.size
  }

  // Linear builder for TreeMap
  // Verify the entries are in order
  def fromOrderedEntries[K, V](entries: IndexedSeqView[(K, V)])(implicit
      ordering: Ordering[K]
  ): TreeMap[K, V] =
    collection.immutable.TreeMap.from(new SortedListAdapter(entries, ordering))

}

