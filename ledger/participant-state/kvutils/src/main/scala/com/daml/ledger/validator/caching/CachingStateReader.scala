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
    cache: Cache[Key, Value],
    shouldCache: Key => Boolean,
    delegate: StateReader[Key, Value],
) extends StateReader[Key, Value] {
  override def read(
      keys: Seq[Key]
  )(implicit executionContext: ExecutionContext): Future[Seq[Value]] = {
    @SuppressWarnings(Array("org.wartremover.warts.Any")) // Required to make `.view` work.
    val cachedValues = keys.view
      .map(key => key -> cache.getIfPresent(key))
      .collect { case (key, Some(value)) => key -> value }
      .toMap
    val keysToRead = keys.toSet -- cachedValues.keySet
    if (keysToRead.nonEmpty) {
      delegate
        .read(keysToRead.toSeq)
        .map { readStateValues =>
          val readValues = keysToRead.zip(readStateValues).toMap
          readValues.foreach {
            case (key, value) =>
              if (shouldCache(key)) {
                cache.put(key, value)
              }
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
    new CachingStateReader[Key, Option[Value]](
      cache = cache.mapValues[Option[Value]](
        from = value => Some(value),
        to = identity,
      ),
      shouldCache = cachingPolicy.shouldCacheOnRead,
      delegate = ledgerStateReader,
    )
}
