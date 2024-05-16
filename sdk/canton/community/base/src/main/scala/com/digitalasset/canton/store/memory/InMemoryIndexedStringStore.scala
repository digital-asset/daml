// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.store.{IndexedStringStore, IndexedStringType}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future, blocking}

/** In memory version of an indexed string store.
  *
  * @param minIndex the minimum value of assigned indices (for testing purposes)
  * @param maxIndex the maximum value of assigned indices (for testing purposes)
  */
class InMemoryIndexedStringStore(val minIndex: Int, val maxIndex: Int) extends IndexedStringStore {

  private val cache = TrieMap[(String300, Int), Int]()
  private val list = ArrayBuffer[(String300, Int)]()

  override def getOrCreateIndex(dbTyp: IndexedStringType, str: String300): Future[Int] =
    Future.successful(getOrCreateIndexForTesting(dbTyp, str))

  /** @throws java.lang.IllegalArgumentException if a new index is created and the new index would exceed `maxIndex`
    */
  def getOrCreateIndexForTesting(dbTyp: IndexedStringType, str: String300): Int = blocking {
    synchronized {
      val key = (str, dbTyp.source)
      cache.get(key) match {
        case Some(value) => value
        case None =>
          val idx = list.length + minIndex
          require(idx <= maxIndex, s"New index $idx would exceed the maximum index $maxIndex.")
          list.append(key).discard
          cache.put(key, idx).discard
          idx
      }
    }
  }

  override def getForIndex(dbTyp: IndexedStringType, idx: Int): Future[Option[String300]] =
    Future.successful {
      blocking {
        synchronized {
          val positionInList = idx - minIndex
          if (positionInList >= 0 && list.lengthCompare(positionInList) > 0) {
            val (str, source) = list(positionInList)
            if (source == dbTyp.source) Some(str) else None
          } else None
        }
      }
    }

  override def close(): Unit = ()
}

object InMemoryIndexedStringStore {
  // Start with 1 by default to have same behavior as the db backed store.
  def apply(): InMemoryIndexedStringStore = new InMemoryIndexedStringStore(1, Int.MaxValue)
}
