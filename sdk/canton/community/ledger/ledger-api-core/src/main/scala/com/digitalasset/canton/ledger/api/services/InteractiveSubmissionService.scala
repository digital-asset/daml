// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.services

import com.daml.ledger.api.v2.interactive_submission_data.PreparedTransaction
import com.daml.ledger.api.v2.interactive_submission_service.{
  ExecuteSubmissionResponse,
  PrepareSubmissionResponse,
}
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.{
  ExecuteRequest,
  PrepareRequest,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.protocol.TransactionAuthorizationPartySignatures
import com.digitalasset.daml.lf.data.Ref.{SubmissionId, WorkflowId}

import scala.concurrent.Future

object InteractiveSubmissionService {
  final case class PrepareRequest(commands: domain.Commands)
  final case class ExecuteRequest(
      submissionId: SubmissionId,
      workflowId: Option[WorkflowId],
      deduplicationPeriod: DeduplicationPeriod,
      partiesSignatures: TransactionAuthorizationPartySignatures,
      preparedTransaction: PreparedTransaction,
  )
}

trait InteractiveSubmissionService {
  def prepare(request: PrepareRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PrepareSubmissionResponse]

  def execute(
      request: ExecuteRequest
  )(implicit loggingContext: LoggingContextWithTrace): Future[ExecuteSubmissionResponse]
}
