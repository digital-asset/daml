// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.daml.ledger.api.v1.command_submission_service.*
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.CommandsValidator
import com.google.protobuf.empty.Empty
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

  override def submit(request: SubmitRequest): Future[Empty] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request.commands)
    authorizer.requireActAndReadClaimsForParties(
      actAs = effectiveSubmitters.actAs,
      readAs = effectiveSubmitters.readAs,
      applicationIdL = Lens.unit[SubmitRequest].commands.applicationId,
      call = service.submit,
    )(request)
  }

  override def bindService(): ServerServiceDefinition =
    CommandSubmissionServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()

}
