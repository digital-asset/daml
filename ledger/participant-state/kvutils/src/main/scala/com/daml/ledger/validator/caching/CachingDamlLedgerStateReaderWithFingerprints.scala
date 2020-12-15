// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.caching.{Cache, Weight}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.Fingerprint
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.RawToDamlLedgerStateReaderAdapter.deserializeDamlStateValue
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.ledger.validator.reading.StateReader
import com.google.protobuf.MessageLite

object CachingDamlLedgerStateReaderWithFingerprints {

  implicit object `Message-Fingerprint Pair Weight` extends Weight[(MessageLite, Fingerprint)] {
    override def weigh(value: (MessageLite, Fingerprint)): Cache.Size =
      value._1.getSerializedSize.toLong + value._2.size()
  }

  def apply(
      cache: Cache[DamlStateKey, (DamlStateValue, Fingerprint)],
      cachingPolicy: CacheUpdatePolicy[DamlStateKey],
      ledgerStateReaderWithFingerprints: StateReader[Key, (Option[Value], Fingerprint)],
      keySerializationStrategy: StateKeySerializationStrategy,
  ): StateReader[DamlStateKey, (Option[DamlStateValue], Fingerprint)] =
    new CachingStateReader[DamlStateKey, (Option[DamlStateValue], Fingerprint)](
      cache = cache.mapValues(
        mapAfterReading = {
          case (value, fingerprint) => (Some(value), fingerprint)
        },
        mapBeforeWriting = {
          case (None, _) => None
          case (Some(value), fingerprint) => Some((value, fingerprint))
        }
      ),
      shouldCache = cachingPolicy.shouldCacheOnRead,
      delegate = ledgerStateReaderWithFingerprints
        .contramapKeys(keySerializationStrategy.serializeStateKey)
        .mapValues {
          case (valueMaybe, fingerprint) =>
            valueMaybe.map(deserializeDamlStateValue) -> fingerprint
        },
    )
}
