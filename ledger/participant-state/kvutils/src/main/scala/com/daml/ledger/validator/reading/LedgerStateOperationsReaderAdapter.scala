// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.reading.StateReader

import scala.concurrent.{ExecutionContext, Future}

final class LedgerStateOperationsReaderAdapter[LogResult](
    operations: LedgerStateOperations[LogResult]
) extends StateReader[Key, Option[Value]] {
  override def read(
      keys: Iterable[Key]
  )(implicit executionContext: ExecutionContext): Future[Seq[Option[Value]]] =
    operations.readState(keys)
}
