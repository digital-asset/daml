// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.validator.reading.StateReader
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

final class LedgerStateOperationsReaderAdapter[LogResult](
    operations: LedgerStateOperations[LogResult]
) extends StateReader[Raw.StateKey, Option[Raw.Envelope]] {
  override def read(
      keys: Iterable[Raw.StateKey]
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Seq[Option[Raw.Envelope]]] =
    operations.readState(keys)
}
