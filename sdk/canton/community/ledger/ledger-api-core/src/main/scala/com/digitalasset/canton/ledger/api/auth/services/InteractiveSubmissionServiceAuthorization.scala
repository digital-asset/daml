// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.interactive.interactive_submission_service.InteractiveSubmissionServiceGrpc.InteractiveSubmissionService
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionRequest,
  ExecuteSubmissionResponse,
  InteractiveSubmissionServiceGrpc,
  PrepareSubmissionRequest,
  PrepareSubmissionResponse,
}
import com.digitalasset.canton.auth.Authorizer
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.CommandsValidator
import io.grpc.ServerServiceDefinition
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

/** Enforce authorization for the interactive submission service using LAPI User management.
  */
final class InteractiveSubmissionServiceAuthorization(
    protected val service: InteractiveSubmissionService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends InteractiveSubmissionService
    with ProxyCloseable
    with GrpcApiService {

  override def prepareSubmission(
      request: PrepareSubmissionRequest
  ): Future[PrepareSubmissionResponse] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request)
    authorizer.requireActAndReadClaimsForParties(
      actAs = Set.empty, // At preparation time the actAs parties are only reading
      readAs = effectiveSubmitters.readAs ++ effectiveSubmitters.actAs,
      applicationIdL = Lens.unit[PrepareSubmissionRequest].applicationId,
      call = service.prepareSubmission,
    )(request)
  }

  override def executeSubmission(
      request: ExecuteSubmissionRequest
  ): Future[ExecuteSubmissionResponse] = {
    val actAsO = for {
      preparedTx <- request.preparedTransaction
      metadata <- preparedTx.metadata
      submitterInfo <- metadata.submitterInfo
    } yield submitterInfo.actAs

    val actAs = actAsO.getOrElse(Seq.empty)

    authorizer.requireActAndReadClaimsForParties(
      actAs = actAs.toSet[String],
      readAs = Set.empty[String],
      applicationIdL = Lens.unit[ExecuteSubmissionRequest].applicationId,
      call = service.executeSubmission,
    )(request)
  }

  override def bindService(): ServerServiceDefinition =
    InteractiveSubmissionServiceGrpc.bindService(this, executionContext)
}
