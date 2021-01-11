// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.kvutils.export.WriteSet
import com.daml.ledger.participant.state.v1.ReadService
import com.daml.ledger.validator.reading.DamlLedgerStateReader
import com.daml.ledger.validator.{CommitStrategy, StateKeySerializationStrategy}

trait QueryableWriteSet {
  def getAndClearRecordedWriteSet(): WriteSet
}

/** A ReadService that streams back previously recorded state updates */
trait ReplayingReadService extends ReadService {
  def updateCount(): Long
}

/** Records state updates and creates corresponding ReplayingReadService instances */
trait ReplayingReadServiceFactory {
  def appendBlock(writeSet: WriteSet): Unit

  def createReadService(implicit materializer: Materializer): ReplayingReadService
}

trait CommitStrategySupport[LogResult] {
  def stateKeySerializationStrategy: StateKeySerializationStrategy

  def ledgerStateReader: DamlLedgerStateReader

  def commitStrategy: CommitStrategy[LogResult]

  def writeSet: QueryableWriteSet

  def newReadServiceFactory(): ReplayingReadServiceFactory

  /**
    * Determines if there's an actual difference and tries to explain it in case there is.
    * A None return value signals that no difference should be signaled to the user.
    */
  def explainMismatchingValue(
      key: Raw.Key,
      expectedValue: Raw.Value,
      actualValue: Raw.Value,
  ): Option[String]

  /**
    * Validates that a single write set entry is readable.
    *
    * @param rawKey   The serialized key.
    * @param rawValue The serialized value.
    * @return `Right(())` if the entry is valid, or `Left(message)` with an explanation otherwise.
    */
  def checkEntryIsReadable(rawKey: Raw.Key, rawValue: Raw.Value): Either[String, Unit]
}
