// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.validator.LedgerStateWriteOperations

import scala.concurrent.{ExecutionContext, Future}

final class SubmissionAggregatorWriteOperations(builder: SubmissionAggregator.WriteSetBuilder)
    extends LedgerStateWriteOperations[Unit] {

  override def writeState(
      key: Raw.Key,
      value: Raw.Value,
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    Future {
      builder += key -> value
    }

  override def writeState(
      keyValuePairs: Iterable[(Raw.Key, Raw.Value)]
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    Future {
      builder ++= keyValuePairs
    }

  override def appendToLog(
      key: Raw.Key,
      value: Raw.Value,
  )(implicit executionContext: ExecutionContext): Future[Unit] =
    Future {
      builder += key -> value
    }
}
