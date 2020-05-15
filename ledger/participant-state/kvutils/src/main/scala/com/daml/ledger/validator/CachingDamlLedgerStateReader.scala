// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.validator.LedgerStateOperations.Key
import com.github.benmanes.caffeine.cache.Cache

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._

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
class CachingDamlLedgerStateReader(
    val cache: Cache[DamlStateKey, DamlStateValue],
    keySerializationStrategy: StateKeySerializationStrategy,
    delegate: DamlLedgerStateReader)(implicit executionContext: ExecutionContext)
    extends DamlLedgerStateReader
    with QueryableReadSet {

  private val readSet = mutable.Set.empty[DamlStateKey]

  override def getReadSet: Set[Key] =
    this.synchronized { readSet.map(keySerializationStrategy.serializeStateKey).toSet }

  override def readState(keys: Seq[DamlStateKey]): Future[Seq[Option[DamlStateValue]]] = {
    this.synchronized { readSet ++= keys }
    val cachedValues = cache.getAllPresent(keys.asJava).asScala.map {
      case (key, value) => key -> Some(value)
    }
    val keysToRead = keys.toSet -- cachedValues.keySet
    if (keysToRead.nonEmpty) {
      delegate
        .readState(keysToRead.toSeq)
        .map { readStateValues =>
          val readValues = keysToRead.zip(readStateValues).toMap
          readValues.collect {
            case (key, Some(value)) => cache.put(key, value)
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
  private[validator] def apply(
      cache: Cache[DamlStateKey, DamlStateValue],
      ledgerStateOperations: LedgerStateReader,
      keySerializationStrategy: StateKeySerializationStrategy)(
      implicit executionContext: ExecutionContext): CachingDamlLedgerStateReader = {
    new CachingDamlLedgerStateReader(
      cache,
      keySerializationStrategy,
      new RawToDamlLedgerStateReaderAdapter(ledgerStateOperations, keySerializationStrategy))
  }
}
