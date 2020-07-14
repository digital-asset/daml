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
    val cache: Cache[DamlStateKey, (Option[DamlStateValue], Fingerprint)],
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
        case (key, Some(valueFingerprintPair)) => (key, valueFingerprintPair)
      }
      .toMap
    val keysToRead = keys.toSet -- cachedValues.keySet
    if (keysToRead.nonEmpty) {
      delegate
        .read(keysToRead.toSeq)
        .map { readResults =>
          assert(keysToRead.size == readResults.size)
          val readValues = keysToRead.zip(readResults).toMap
          readValues.collect {
            case (key, valueFingerprintPair) if shouldCache(key) =>
              cache.put(key, valueFingerprintPair)
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
