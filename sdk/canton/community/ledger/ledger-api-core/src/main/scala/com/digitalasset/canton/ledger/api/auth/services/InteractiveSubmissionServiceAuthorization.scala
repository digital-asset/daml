// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.interactive.interactive_submission_service.InteractiveSubmissionServiceGrpc.InteractiveSubmissionService
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionAndWaitForTransactionRequest,
  ExecuteSubmissionAndWaitForTransactionResponse,
  ExecuteSubmissionAndWaitRequest,
  ExecuteSubmissionAndWaitResponse,
  ExecuteSubmissionRequest,
  ExecuteSubmissionResponse,
  GetPreferredPackageVersionRequest,
  GetPreferredPackageVersionResponse,
  GetPreferredPackagesRequest,
  GetPreferredPackagesResponse,
  InteractiveSubmissionServiceGrpc,
  PrepareSubmissionRequest,
  PrepareSubmissionResponse,
  PreparedTransaction,
}
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
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

  import InteractiveSubmissionServiceAuthorization.*

  override def prepareSubmission(
      request: PrepareSubmissionRequest
  ): Future[PrepareSubmissionResponse] =
    authorizer.rpc(service.prepareSubmission)(
      getPreparedSubmissionClaims(request)*
    )(request)

  override def executeSubmission(
      request: ExecuteSubmissionRequest
  ): Future[ExecuteSubmissionResponse] =
    authorizer.rpc(service.executeSubmission)(
      getExecuteSubmissionClaims(
        request,
        preparedTransactionForExecuteSubmissionL,
        userIdForExecuteSubmissionL,
      )*
    )(request)

  override def getPreferredPackageVersion(
      request: GetPreferredPackageVersionRequest
  ): Future[GetPreferredPackageVersionResponse] =
    authorizer.rpc(service.getPreferredPackageVersion)(RequiredClaim.Public())(request)

  override def getPreferredPackages(
      request: GetPreferredPackagesRequest
  ): Future[GetPreferredPackagesResponse] =
    authorizer.rpc(service.getPreferredPackages)(RequiredClaim.Public())(request)

  override def bindService(): ServerServiceDefinition =
    InteractiveSubmissionServiceGrpc.bindService(this, executionContext)

  override def executeSubmissionAndWait(
      request: ExecuteSubmissionAndWaitRequest
  ): Future[ExecuteSubmissionAndWaitResponse] =
    authorizer.rpc(service.executeSubmissionAndWait)(
      getExecuteSubmissionClaims(
        request,
        preparedTransactionForExecuteSubmissionAndWaitL,
        userIdForExecuteSubmissionAndWaitL,
      )*
    )(request)

  override def executeSubmissionAndWaitForTransaction(
      request: ExecuteSubmissionAndWaitForTransactionRequest
  ): Future[ExecuteSubmissionAndWaitForTransactionResponse] =
    authorizer.rpc(service.executeSubmissionAndWaitForTransaction)(
      getExecuteSubmissionAndWaitForTransactionClaims(request)*
    )(request)
}

object InteractiveSubmissionServiceAuthorization {

  def getPreparedSubmissionClaims(
      request: PrepareSubmissionRequest
  ): List[RequiredClaim[PrepareSubmissionRequest]] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request)
    RequiredClaims.executionClaims(
      executeAs = Set.empty, // At preparation time the executeAs parties are only reading
      readAs = effectiveSubmitters.readAs ++ effectiveSubmitters.actAs,
      userIdL = userIdForPrepareSubmissionL,
    )
  }

  def getExecuteSubmissionClaims[Req](
      request: Req,
      preparedTransactionL: Lens[Req, Option[PreparedTransaction]],
      userIdL: Lens[Req, String],
  ): List[RequiredClaim[Req]] = {
    val executeAsO = for {
      preparedTx <- preparedTransactionL.get(request)
      metadata <- preparedTx.metadata
      submitterInfo <- metadata.submitterInfo
    } yield submitterInfo.actAs
    val executeAs = executeAsO.getOrElse(Seq.empty)
    RequiredClaims.executionClaims(
      executeAs = executeAs.toSet[String],
      readAs = Set.empty[String],
      userIdL = userIdL,
    )
  }

  def getExecuteSubmissionAndWaitForTransactionClaims(
      request: ExecuteSubmissionAndWaitForTransactionRequest
  ): List[RequiredClaim[ExecuteSubmissionAndWaitForTransactionRequest]] =
    (getExecuteSubmissionClaims(
      request,
      preparedTransactionForExecuteSubmissionAndWaitForTransactionL,
      userIdForExecuteSubmissionAndWaitForTransactionL,
    ) ::: request.transactionFormat.toList
      .flatMap(
        RequiredClaims.transactionFormatClaims[ExecuteSubmissionAndWaitForTransactionRequest]
      )).distinct

  val userIdForPrepareSubmissionL: Lens[PrepareSubmissionRequest, String] =
    Lens.unit[PrepareSubmissionRequest].userId
  val preparedTransactionForExecuteSubmissionL
      : Lens[ExecuteSubmissionRequest, Option[PreparedTransaction]] =
    Lens.unit[ExecuteSubmissionRequest].optionalPreparedTransaction
  val userIdForExecuteSubmissionL: Lens[ExecuteSubmissionRequest, String] =
    Lens.unit[ExecuteSubmissionRequest].userId
  val preparedTransactionForExecuteSubmissionAndWaitL
      : Lens[ExecuteSubmissionAndWaitRequest, Option[PreparedTransaction]] =
    Lens.unit[ExecuteSubmissionAndWaitRequest].optionalPreparedTransaction
  val userIdForExecuteSubmissionAndWaitL: Lens[ExecuteSubmissionAndWaitRequest, String] =
    Lens.unit[ExecuteSubmissionAndWaitRequest].userId
  val preparedTransactionForExecuteSubmissionAndWaitForTransactionL
      : Lens[ExecuteSubmissionAndWaitForTransactionRequest, Option[PreparedTransaction]] =
    Lens.unit[ExecuteSubmissionAndWaitForTransactionRequest].optionalPreparedTransaction
  val userIdForExecuteSubmissionAndWaitForTransactionL
      : Lens[ExecuteSubmissionAndWaitForTransactionRequest, String] =
    Lens.unit[ExecuteSubmissionAndWaitForTransactionRequest].userId
}
