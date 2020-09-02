// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.validator.LedgerStateOperations.Key
import com.daml.ledger.validator.caching.CachingDamlLedgerStateReader.StateCache
import com.daml.ledger.validator.{
  DamlLedgerStateReader,
  LedgerStateReader,
  StateKeySerializationStrategy
}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Queries the set of state reads that have been performed.
  * Must include both cached and actual reads.
  * Useful when the read set is required for e.g. conflict detection. */
trait QueryableReadSet {
  def getReadSet: Set[Key]
}

/** A caching adapter for ledger read operations.
  *
  * This is crucial for caching access to large frequently accessed state, for example
  * package state values (which are too expensive to deserialize from bytes every time).
  */
final class CachingDamlLedgerStateReader(
    val cache: StateCache,
    shouldCache: DamlStateKey => Boolean,
    keySerializationStrategy: StateKeySerializationStrategy,
    delegate: DamlLedgerStateReader,
)(implicit executionContext: ExecutionContext)
    extends DamlLedgerStateReader
    with QueryableReadSet {

  private val readSet = mutable.Set.empty[DamlStateKey]

  override def getReadSet: Set[Key] =
    this.synchronized { readSet.map(keySerializationStrategy.serializeStateKey).toSet }

  override def readState(keys: Seq[DamlStateKey]): Future[Seq[Option[DamlStateValue]]] = {
    this.synchronized { readSet ++= keys }
    @SuppressWarnings(Array("org.wartremover.warts.Any")) // Required to make `.view` work.
    val cachedValues = keys.view
      .map(key => key -> cache.getIfPresent(key))
      .filter(_._2.isDefined)
      .toMap
    val keysToRead = keys.toSet -- cachedValues.keySet
    if (keysToRead.nonEmpty) {
      delegate
        .readState(keysToRead.toSeq)
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

object CachingDamlLedgerStateReader {

  type StateCache = Cache[DamlStateKey, DamlStateValue]

  def apply(
      cache: StateCache,
      cachingPolicy: CacheUpdatePolicy,
      ledgerStateOperations: LedgerStateReader,
      keySerializationStrategy: StateKeySerializationStrategy,
  )(implicit executionContext: ExecutionContext): CachingDamlLedgerStateReader =
    new CachingDamlLedgerStateReader(
      cache,
      cachingPolicy.shouldCacheOnRead,
      keySerializationStrategy,
      DamlLedgerStateReader.from(ledgerStateOperations, keySerializationStrategy),
    )
}
