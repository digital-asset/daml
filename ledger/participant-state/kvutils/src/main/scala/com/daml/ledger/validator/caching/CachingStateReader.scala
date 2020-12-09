// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.caching.Cache
import com.daml.ledger.validator.reading.StateReader

import scala.concurrent.{ExecutionContext, Future}

/** A caching adapter for ledger read operations.
  *
  * This is crucial for caching access to large frequently accessed state, for example
  * package state values (which are too expensive to deserialize from bytes every time).
  */
final class CachingStateReader[Key, Value](
    val cache: Cache[Key, Value],
    shouldCache: Key => Boolean,
    delegate: StateReader[Key, Option[Value]],
) extends StateReader[Key, Option[Value]] {
  override def read(
      keys: Seq[Key]
  )(implicit executionContext: ExecutionContext): Future[Seq[Option[Value]]] = {
    @SuppressWarnings(Array("org.wartremover.warts.Any")) // Required to make `.view` work.
    val cachedValues = keys.view
      .map(key => key -> cache.getIfPresent(key))
      .filter(_._2.isDefined)
      .toMap
    val keysToRead = keys.toSet -- cachedValues.keySet
    if (keysToRead.nonEmpty) {
      delegate
        .read(keysToRead.toSeq)
        .map { readStateValues =>
          val readValues = keysToRead.zip(readStateValues).toMap
          readValues.collect {
            case (key, Some(value)) if shouldCache(key) => cache.put(key, value)
          }
          val all = cachedValues ++ readValues
          keys.map(all(_))
        }
    } else {
      Future {
        keys.map(cachedValues(_))
      }
    }
  }
}

object CachingStateReader {
  def apply[Key, Value](
      cache: Cache[Key, Value],
      cachingPolicy: CacheUpdatePolicy[Key],
      ledgerStateReader: StateReader[Key, Option[Value]],
  ): StateReader[Key, Option[Value]] =
    new CachingStateReader(
      cache,
      cachingPolicy.shouldCacheOnRead,
      ledgerStateReader,
    )
}
