// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_service.*
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.CommandsValidator
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

/** Note: the command service internally uses calls to the CommandSubmissionService and CommandCompletionService.
  * These calls already require authentication, but it is better to check authorization here as well.
  */
final class CommandServiceAuthorization(
    protected val service: CommandService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends CommandService
    with ProxyCloseable
    with GrpcApiService {

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request.commands)
    authorizer.requireActAndReadClaimsForParties(
      actAs = effectiveSubmitters.actAs,
      readAs = effectiveSubmitters.readAs,
      applicationIdL = Lens.unit[SubmitAndWaitRequest].commands.applicationId,
      call = service.submitAndWait,
    )(request)
  }

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request.commands)
    authorizer.requireActAndReadClaimsForParties(
      actAs = effectiveSubmitters.actAs,
      readAs = effectiveSubmitters.readAs,
      applicationIdL = Lens.unit[SubmitAndWaitRequest].commands.applicationId,
      call = service.submitAndWaitForTransaction,
    )(request)
  }

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionIdResponse] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request.commands)
    authorizer.requireActAndReadClaimsForParties(
      actAs = effectiveSubmitters.actAs,
      readAs = effectiveSubmitters.readAs,
      applicationIdL = Lens.unit[SubmitAndWaitRequest].commands.applicationId,
      call = service.submitAndWaitForTransactionId,
    )(request)
  }

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request.commands)
    authorizer.requireActAndReadClaimsForParties(
      actAs = effectiveSubmitters.actAs,
      readAs = effectiveSubmitters.readAs,
      applicationIdL = Lens.unit[SubmitAndWaitRequest].commands.applicationId,
      call = service.submitAndWaitForTransactionTree,
    )(request)
  }

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()

}
