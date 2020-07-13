package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.{DamlKvutils, Fingerprint}
import com.daml.ledger.validator.StateKeySerializationStrategy

import scala.concurrent.{ExecutionContext, Future}

import com.daml.ledger.validator.RawToDamlLedgerStateReaderAdapter.deserializeDamlStateValue

private[validator] class RawToDamlLedgerStateReaderWithFingerprintsAdapter(
    ledgerStateReaderWithFingerprints: LedgerStateReaderWithFingerprints,
    keySerializationStrategy: StateKeySerializationStrategy
)(implicit executionContext: ExecutionContext)
    extends DamlLedgerStateReaderWithFingerprints {
  override def read(keys: Seq[DamlKvutils.DamlStateKey])
    : Future[Seq[(Option[DamlKvutils.DamlStateValue], Fingerprint)]] =
    ledgerStateReaderWithFingerprints
      .read(keys.map(keySerializationStrategy.serializeStateKey))
      .map(_.map {
        case (valueMaybe, fingerprint) =>
          valueMaybe.map(deserializeDamlStateValue) -> fingerprint
      })
}
