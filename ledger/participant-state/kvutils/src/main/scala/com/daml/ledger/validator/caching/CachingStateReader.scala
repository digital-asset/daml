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
final class CachingStateReader[Key, Value, CachedValue](
    cache: Cache[Key, CachedValue],
    shouldCache: Key => Boolean,
    delegate: StateReader[Key, Value],
    toCached: Value => Option[CachedValue],
    fromCached: CachedValue => Value,
) extends StateReader[Key, Value] {
  override def read(
      keys: Seq[Key]
  )(implicit executionContext: ExecutionContext): Future[Seq[Value]] = {
    @SuppressWarnings(Array("org.wartremover.warts.Any")) // Required to make `.view` work.
    val cachedValues = keys.view
      .map(key => key -> cache.getIfPresent(key))
      .collect { case (key, Some(value)) => key -> fromCached(value) }
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
                toCached(value) match {
                  case None => ()
                  case Some(cachedValue) =>
                    cache.put(key, cachedValue)
                }
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
    new CachingStateReader[Key, Option[Value], Value](
      cache = cache,
      shouldCache = cachingPolicy.shouldCacheOnRead,
      delegate = ledgerStateReader,
      toCached = identity,
      fromCached = Some.apply,
    )
}
