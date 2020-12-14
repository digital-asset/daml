// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.v1.SubmissionResult
import com.daml.ledger.validator.LedgerStateOperations

import scala.concurrent.{ExecutionContext, Future}

trait PostExecutionFinalizer[ReadSet, WriteSet] {
  def finalizeSubmission[LogResult](
      preExecutionOutput: PreExecutionOutput[ReadSet, WriteSet],
      ledgerStateOperations: LedgerStateOperations[LogResult],
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult]
}
