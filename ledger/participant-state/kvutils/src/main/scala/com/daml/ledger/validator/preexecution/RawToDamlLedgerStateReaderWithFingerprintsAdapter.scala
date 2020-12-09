// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.{DamlKvutils, Fingerprint}
import com.daml.ledger.validator.RawToDamlLedgerStateReaderAdapter.deserializeDamlStateValue
import com.daml.ledger.validator.StateKeySerializationStrategy

import scala.concurrent.{ExecutionContext, Future}

private[validator] class RawToDamlLedgerStateReaderWithFingerprintsAdapter(
    ledgerStateReaderWithFingerprints: LedgerStateReaderWithFingerprints,
    keySerializationStrategy: StateKeySerializationStrategy
) extends DamlLedgerStateReaderWithFingerprints {
  override def read(
      keys: Seq[DamlKvutils.DamlStateKey]
  )(implicit executionContext: ExecutionContext)
    : Future[Seq[(Option[DamlKvutils.DamlStateValue], Fingerprint)]] =
    ledgerStateReaderWithFingerprints
      .read(keys.map(keySerializationStrategy.serializeStateKey))
      .map(_.map {
        case (valueMaybe, fingerprint) =>
          valueMaybe.map(deserializeDamlStateValue) -> fingerprint
      })
}
