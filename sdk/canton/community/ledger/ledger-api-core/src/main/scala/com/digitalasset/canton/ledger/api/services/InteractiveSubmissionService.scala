// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.services

import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionResponse,
  PrepareSubmissionResponse,
  PreparedTransaction,
}
import com.digitalasset.canton.LfTimestamp
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod}
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.{
  ExecuteRequest,
  PrepareRequest,
}
import com.digitalasset.canton.ledger.api.validation.GetPreferredPackagesRequestValidator.PackageVettingRequirements
import com.digitalasset.canton.ledger.api.{Commands, PackageReference}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.data.Ref.{SubmissionId, UserId}

object InteractiveSubmissionService {
  final case class PrepareRequest(
      commands: Commands,
      verboseHashing: Boolean,
      maxRecordTime: Option[LfTimestamp],
  )

  final case class ExecuteRequest(
      userId: UserId,
      submissionId: SubmissionId,
      deduplicationPeriod: DeduplicationPeriod,
      signatures: Map[PartyId, Seq[Signature]],
      preparedTransaction: PreparedTransaction,
      serializationVersion: HashingSchemeVersion,
      synchronizerId: SynchronizerId,
      tentativeLedgerEffectiveTime: LfTimestamp,
  )
}

trait InteractiveSubmissionService {
  def prepare(request: PrepareRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[PrepareSubmissionResponse]

  def execute(request: ExecuteRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[ExecuteSubmissionResponse]

  def getPreferredPackages(
      packageVettingRequirements: PackageVettingRequirements,
      synchronizerId: Option[SynchronizerId],
      vettingValidAt: Option[CantonTimestamp],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[Either[String, (Seq[PackageReference], SynchronizerId)]]
}
