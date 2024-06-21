// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.daml.ledger.api.v2.command_submission_service.*
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.CommandsValidator
import io.grpc.ServerServiceDefinition
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

final class CommandSubmissionServiceAuthorization(
    protected val service: CommandSubmissionService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends CommandSubmissionService
    with ProxyCloseable
    with GrpcApiService {

  override def submit(request: SubmitRequest): Future[SubmitResponse] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmittersV2(request.commands)
    authorizer.requireActAndReadClaimsForParties(
      actAs = effectiveSubmitters.actAs,
      readAs = effectiveSubmitters.readAs,
      applicationIdL = Lens.unit[SubmitRequest].commands.applicationId,
      call = service.submit,
    )(request)
  }

  override def submitReassignment(
      request: SubmitReassignmentRequest
  ): Future[SubmitReassignmentResponse] =
    authorizer
      .requireActAndReadClaimsForParties(
        actAs = request.reassignmentCommand.map(_.submitter).toList.toSet,
        readAs = Set.empty,
        applicationIdL = Lens.unit[SubmitReassignmentRequest].reassignmentCommand.applicationId,
        call = service.submitReassignment,
      )(request)

  override def bindService(): ServerServiceDefinition =
    CommandSubmissionServiceGrpc.bindService(this, executionContext)

}
