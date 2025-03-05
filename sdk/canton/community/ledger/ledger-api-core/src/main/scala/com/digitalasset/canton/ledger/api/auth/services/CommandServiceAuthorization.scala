// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.command_service.*
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
import com.digitalasset.canton.ledger.api.auth.services.CommandServiceAuthorization.getSubmitAndWaitForTransactionClaims
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.CommandsValidator
import io.grpc.ServerServiceDefinition
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

/** Note: the command service internally uses calls to the CommandSubmissionService and
  * CommandCompletionService. These calls already require authentication, but it is better to check
  * authorization here as well.
  */
final class CommandServiceAuthorization(
    protected val service: CommandService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends CommandService
    with ProxyCloseable
    with GrpcApiService {

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitForTransactionRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    authorizer.rpc(service.submitAndWaitForTransaction)(
      getSubmitAndWaitForTransactionClaims(request)*
    )(request)

  override def submitAndWait(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitResponse] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request.commands)
    authorizer.rpc(service.submitAndWait)(
      RequiredClaims.submissionClaims(
        actAs = effectiveSubmitters.actAs,
        readAs = effectiveSubmitters.readAs,
        applicationIdL = Lens.unit[SubmitAndWaitRequest].commands.applicationId,
      )*
    )(request)
  }

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request.commands)
    authorizer.rpc(service.submitAndWaitForTransactionTree)(
      RequiredClaims.submissionClaims(
        actAs = effectiveSubmitters.actAs,
        readAs = effectiveSubmitters.readAs,
        applicationIdL = Lens.unit[SubmitAndWaitRequest].commands.applicationId,
      )*
    )(request)
  }

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, executionContext)
}

object CommandServiceAuthorization {
  def getSubmitAndWaitForTransactionClaims(
      request: SubmitAndWaitForTransactionRequest
  ): List[RequiredClaim[SubmitAndWaitForTransactionRequest]] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request.commands)
    (RequiredClaims.submissionClaims(
      actAs = effectiveSubmitters.actAs,
      readAs = effectiveSubmitters.readAs,
      applicationIdL = applicationIdForTransactionL,
    ) ::: request.transactionFormat.toList
      .flatMap(
        RequiredClaims.transactionFormatClaims[SubmitAndWaitForTransactionRequest]
      )).distinct
  }

  val applicationIdL: Lens[SubmitAndWaitRequest, String] =
    Lens.unit[SubmitAndWaitRequest].commands.applicationId

  val applicationIdForTransactionL: Lens[SubmitAndWaitForTransactionRequest, String] =
    Lens.unit[SubmitAndWaitForTransactionRequest].commands.applicationId

}
