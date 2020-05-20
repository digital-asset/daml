// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}

import scala.concurrent.{ExecutionContext, Future}

/** Defines how we read from an underlying ledger, using internal kvutils types.
  * This is the interface that the validator works against. However, ledger
  * integrations need not implement this directly.
  * You can create a DamlLedgerStateReader instance via the factory methods
  * available in [[com.daml.ledger.validator.batch.BatchedSubmissionValidatorFactory]]
  * We're required to work at this level of abstraction in order to implement
  * efficient caching (e.g. package DamlStateValue is too large to be always
  * decompressed and deserialized from bytes).
  */
trait DamlLedgerStateReader {
  def readState(keys: Seq[DamlStateKey]): Future[Seq[Option[DamlStateValue]]]
}

private[validator] object DamlLedgerStateReader {
  def from(
      ledgerStateReader: LedgerStateReader,
      keySerializationStrategy: StateKeySerializationStrategy)(
      implicit executionContext: ExecutionContext): DamlLedgerStateReader =
    new RawToDamlLedgerStateReaderAdapter(ledgerStateReader, keySerializationStrategy)
}
