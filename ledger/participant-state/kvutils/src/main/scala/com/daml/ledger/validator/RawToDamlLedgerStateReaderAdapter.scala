// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.validator.reading.{DamlLedgerStateReader, LedgerStateReader}

import scala.concurrent.{ExecutionContext, Future}

final class RawToDamlLedgerStateReaderAdapter(
    ledgerStateReader: LedgerStateReader,
    keySerializationStrategy: StateKeySerializationStrategy,
) extends DamlLedgerStateReader {

  import RawToDamlLedgerStateReaderAdapter.deserializeDamlStateValue

  override def read(
      keys: Iterable[DamlStateKey]
  )(implicit executionContext: ExecutionContext): Future[Seq[Option[DamlStateValue]]] =
    ledgerStateReader
      .read(keys.map(keySerializationStrategy.serializeStateKey))
      .map(_.map(_.map(deserializeDamlStateValue)))
}

object RawToDamlLedgerStateReaderAdapter {
  private[validator] val deserializeDamlStateValue: LedgerStateOperations.Value => DamlStateValue =
    Envelope
      .openStateValue(_)
      .getOrElse(sys.error("Opening enveloped DamlStateValue failed"))
}
