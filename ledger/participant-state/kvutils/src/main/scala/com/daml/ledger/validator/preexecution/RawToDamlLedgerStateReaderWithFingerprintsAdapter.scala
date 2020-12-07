// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey
import com.daml.ledger.participant.state.kvutils.{DamlKvutils, Fingerprint}
import com.daml.ledger.validator.StateKeySerializationStrategy

import scala.concurrent.{ExecutionContext, Future}
import com.daml.ledger.validator.RawToDamlLedgerStateReaderAdapter.deserializeDamlStateValue

private[validator] class RawToDamlLedgerStateReaderWithFingerprintsAdapter(
    ledgerStateReaderWithFingerprints: LedgerStateReaderWithFingerprints,
    keySerializationStrategy: StateKeySerializationStrategy
)(implicit executionContext: ExecutionContext) {
  def read(keys: Seq[DamlStateKey], validateCached: Seq[(DamlStateKey, Fingerprint)])
    : Future[(Seq[(Option[DamlKvutils.DamlStateValue], Fingerprint)], Seq[(DamlStateKey, (Option[DamlKvutils.DamlStateValue], Fingerprint))])] = {
    ledgerStateReaderWithFingerprints
      .read(
        keys.map(keySerializationStrategy.serializeStateKey),
        validateCached.map{case (k,v) => keySerializationStrategy.serializeStateKey(k) -> v})
      .map{
        case (nonCached, invalidatedCached) =>
          (nonCached.map {
            case (valueMaybe, fingerprint) =>
              valueMaybe.map(deserializeDamlStateValue) -> fingerprint
          }, invalidatedCached.map{
            case (key, (maybeValue, fingerprint)) =>
              keySerializationStrategy.deserializeStateKey(key) -> (maybeValue.map(deserializeDamlStateValue) -> fingerprint)
          })
      }
  }
}
