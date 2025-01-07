// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.services

import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionResponse,
  PrepareSubmissionResponse,
  PreparedTransaction,
}
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.{
  ExecuteRequest,
  PrepareRequest,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.data.Ref.{ApplicationId, SubmissionId}

object InteractiveSubmissionService {
  final case class PrepareRequest(commands: domain.Commands, verboseHashing: Boolean)

  final case class ExecuteRequest(
      applicationId: ApplicationId,
      submissionId: SubmissionId,
      deduplicationPeriod: DeduplicationPeriod,
      signatures: Map[PartyId, Seq[Signature]],
      preparedTransaction: PreparedTransaction,
      serializationVersion: HashingSchemeVersion,
      synchronizerId: SynchronizerId,
  )
}

trait InteractiveSubmissionService {
  def prepare(request: PrepareRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[PrepareSubmissionResponse]

  def execute(
      request: ExecuteRequest
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[ExecuteSubmissionResponse]
}
