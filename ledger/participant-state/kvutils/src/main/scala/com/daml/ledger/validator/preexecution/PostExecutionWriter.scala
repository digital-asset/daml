// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.ledger.validator.LedgerStateWriteOperations
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

trait PostExecutionWriter[WriteSet] {
  def write[LogResult](
      writeSet: WriteSet,
      operations: LedgerStateWriteOperations[LogResult],
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[SubmissionResult]
}
