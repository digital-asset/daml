// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{DamlKvutils, Fingerprint}
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.ledger.validator.preexecution.DamlLedgerStateReaderWithFingerprints

import scala.concurrent.{ExecutionContext, Future}

class CachingDamlLedgerStateReaderWithFingerprints(
    val cache: Cache[DamlStateKey, (DamlStateValue, Fingerprint)],
    shouldCache: DamlStateKey => Boolean,
    keySerializationStrategy: StateKeySerializationStrategy,
    delegate: DamlLedgerStateReaderWithFingerprints)(implicit executionContext: ExecutionContext)
    extends DamlLedgerStateReaderWithFingerprints {
  override def read(keys: Seq[DamlKvutils.DamlStateKey])
    : Future[Seq[(Option[DamlKvutils.DamlStateValue], Fingerprint)]] = {
    @SuppressWarnings(Array("org.wartremover.warts.Any")) // Required to make `.view` work.
    val cachedValues: Map[DamlStateKey, (Option[DamlStateValue], Fingerprint)] = keys.view
      .map(key => key -> cache.getIfPresent(key))
      .collect {
        case (key, Some((value, fingerprint))) => (key, (Some(value), fingerprint))
      }
      .toMap
    val keysToRead = keys.toSet -- cachedValues.keySet
    if (keysToRead.nonEmpty) {
      delegate
        .read(keysToRead.toSeq)
        .map { readStateValues =>
          assert(keysToRead.size == readStateValues.size)
          val readValues = keysToRead.zip(readStateValues).toMap
          readValues.collect {
            case (key, (Some(value), fingerprint)) if shouldCache(key) =>
              cache.put(key, value -> fingerprint)
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
