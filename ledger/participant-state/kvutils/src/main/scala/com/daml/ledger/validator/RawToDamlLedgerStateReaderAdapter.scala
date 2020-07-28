// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.Envelope

import scala.concurrent.{ExecutionContext, Future}

private[validator] class RawToDamlLedgerStateReaderAdapter(
    ledgerStateReader: LedgerStateReader,
    keySerializationStrategy: StateKeySerializationStrategy)(
    implicit executionContext: ExecutionContext)
    extends DamlLedgerStateReader {
  override def readState(keys: Seq[DamlStateKey]): Future[Seq[Option[DamlStateValue]]] =
    ledgerStateReader
      .read(keys.map(keySerializationStrategy.serializeStateKey))
      .map(_.map(_.map(deserializeDamlStateValue)))

  private val deserializeDamlStateValue: LedgerStateOperations.Value => DamlStateValue =
    Envelope
      .openStateValue(_)
      .getOrElse(sys.error("Opening enveloped DamlStateValue failed"))
}
