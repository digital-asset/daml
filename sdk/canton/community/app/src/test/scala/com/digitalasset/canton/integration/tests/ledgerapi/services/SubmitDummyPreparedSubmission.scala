// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.services

import com.daml.ledger.api.v2.interactive.interactive_submission_service.ExecuteSubmissionRequest.DeduplicationPeriod
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionAndWaitForTransactionRequest,
  ExecuteSubmissionAndWaitForTransactionResponse,
  ExecuteSubmissionAndWaitRequest,
  ExecuteSubmissionAndWaitResponse,
  ExecuteSubmissionRequest,
  ExecuteSubmissionResponse,
  InteractiveSubmissionServiceGrpc,
  PrepareSubmissionRequest,
  PrepareSubmissionResponse,
}
import com.digitalasset.canton.integration.tests.ledgerapi.auth.ServiceCallWithMainActorAuthTests
import com.digitalasset.canton.serialization.ProtoConverter
import io.scalaland.chimney.dsl.*

import java.util.UUID
import scala.concurrent.Future

trait SubmitDummyPreparedSubmission extends SubmitDummyCommand {
  self: ServiceCallWithMainActorAuthTests =>

  protected def dummyPrepareSubmissionRequest(
      party: String,
      userId: String,
  ): PrepareSubmissionRequest =
    PrepareSubmissionRequest(
      userId = userId,
      commandId = UUID.randomUUID().toString,
      commands = List(
        createWithOperator(templateIds.dummy, party),
        createWithOperator(templateIds.dummyWithParam, party),
        createWithOperator(templateIds.dummyFactory, party),
      ),
      minLedgerTime = None,
      actAs = Seq(party),
      readAs = Seq.empty,
      disclosedContracts = Seq.empty,
      synchronizerId = "",
      packageIdSelectionPreference = Seq.empty,
      verboseHashing = true,
      prefetchContractKeys = Seq.empty,
      maxRecordTime = Option.empty,
      estimateTrafficCost = None,
    )

  protected def dummyExecuteSubmissionRequest(
      userId: String,
      preparedSubmission: PrepareSubmissionResponse,
  ): ExecuteSubmissionRequest =
    ExecuteSubmissionRequest(
      preparedTransaction = preparedSubmission.preparedTransaction,
      partySignatures = None,
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(
        ProtoConverter.DurationConverter.toProtoPrimitive(java.time.Duration.ofSeconds(1))
      ),
      submissionId = UUID.randomUUID().toString,
      userId = userId,
      hashingSchemeVersion = preparedSubmission.hashingSchemeVersion,
      minLedgerTime = None,
    )

  protected def prepareSubmission(
      token: Option[String],
      party: String,
      userId: String,
  ): Future[PrepareSubmissionResponse] =
    stub(InteractiveSubmissionServiceGrpc.stub(channel), token)
      .prepareSubmission(dummyPrepareSubmissionRequest(party, userId))

  protected def executeSubmission(
      token: Option[String],
      userId: String,
      preparedSubmission: PrepareSubmissionResponse,
  ): Future[ExecuteSubmissionResponse] =
    stub(InteractiveSubmissionServiceGrpc.stub(channel), token)
      .executeSubmission(dummyExecuteSubmissionRequest(userId, preparedSubmission))

  protected def executeSubmissionAndWait(
      token: Option[String],
      userId: String,
      preparedSubmission: PrepareSubmissionResponse,
  ): Future[ExecuteSubmissionAndWaitResponse] =
    stub(InteractiveSubmissionServiceGrpc.stub(channel), token)
      .executeSubmissionAndWait(
        dummyExecuteSubmissionRequest(userId, preparedSubmission)
          .transformInto[ExecuteSubmissionAndWaitRequest]
      )

  protected def executeSubmissionAndWaitForTransaction(
      token: Option[String],
      userId: String,
      preparedSubmission: PrepareSubmissionResponse,
  ): Future[ExecuteSubmissionAndWaitForTransactionResponse] =
    stub(InteractiveSubmissionServiceGrpc.stub(channel), token)
      .executeSubmissionAndWaitForTransaction(
        dummyExecuteSubmissionRequest(userId, preparedSubmission)
          .into[ExecuteSubmissionAndWaitForTransactionRequest]
          .withFieldConst(_.transactionFormat, None)
          .transform
      )

}
