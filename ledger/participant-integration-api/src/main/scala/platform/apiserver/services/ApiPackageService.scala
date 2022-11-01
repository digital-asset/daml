// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import com.daml.daml_lf_dev.DamlLf.{Archive, HashFunction}
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.daml.ledger.api.v1.package_service.{HashFunction => APIHashFunction, _}
import com.daml.ledger.api.validation.ValidationErrors
import com.daml.ledger.participant.state.index.v2.IndexPackagesService
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.error.definitions.LedgerApiErrors
import com.daml.platform.server.api.ValidationLogger
import com.daml.platform.server.api.validation.PackageServiceValidation
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiPackageService private (
    backend: IndexPackagesService
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends PackageService
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] = {
    logger.info(s"Received request to list packages: $request")
    backend
      .listLfPackages()
      .map(p => ListPackagesResponse(p.keys.toSeq))
      .andThen(logger.logErrorsOnCall[ListPackagesResponse])
  }

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
    withEnrichedLoggingContext("packageId" -> request.packageId) { implicit loggingContext =>
      logger.info(s"Received request for a package: $request")
      withValidatedPackageId(request.packageId, request) { packageId =>
        backend
          .getLfArchive(packageId)
          .flatMap {
            case None =>
              Future.failed[GetPackageResponse](
                LedgerApiErrors.RequestValidation.NotFound.Package
                  .Reject(packageId = packageId)(
                    createContextualizedErrorLogger
                  )
                  .asGrpcError
              )
            case Some(archive) => Future.successful(toGetPackageResponse(archive))
          }
          .andThen(logger.logErrorsOnCall[GetPackageResponse])
      }
    }

  override def getPackageStatus(
      request: GetPackageStatusRequest
  ): Future[GetPackageStatusResponse] =
    withEnrichedLoggingContext("packageId" -> request.packageId) { implicit loggingContext =>
      logger.info(s"Received request for a package status: $request")
      withValidatedPackageId(request.packageId, request) { packageId =>
        backend
          .listLfPackages()
          .map { packages =>
            val result = if (packages.contains(packageId)) {
              PackageStatus.REGISTERED
            } else {
              PackageStatus.UNKNOWN
            }
            GetPackageStatusResponse(result)
          }
          .andThen(logger.logErrorsOnCall[GetPackageStatusResponse])
      }
    }

  private def withValidatedPackageId[T, R](packageId: String, request: R)(
      block: Ref.PackageId => Future[T]
  )(implicit loggingContext: LoggingContext): Future[T] =
    Ref.PackageId
      .fromString(packageId)
      .fold(
        errorMessage =>
          Future.failed[T](
            ValidationLogger.logFailure(
              request,
              ValidationErrors
                .invalidArgument(s"Invalid package id: $errorMessage")(
                  createContextualizedErrorLogger
                ),
            )
          ),
        packageId => block(packageId),
      )

  private def toGetPackageResponse(archive: Archive): GetPackageResponse = {
    val hashFunction = archive.getHashFunction match {
      case HashFunction.SHA256 => APIHashFunction.SHA256
      case _ => APIHashFunction.Unrecognized(-1)
    }
    GetPackageResponse(
      hashFunction = hashFunction,
      archivePayload = archive.getPayload,
      hash = archive.getHash,
    )
  }

  private def createContextualizedErrorLogger(implicit
      loggingContext: LoggingContext
  ): DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)
}

private[platform] object ApiPackageService {
  def create(
      ledgerId: LedgerId,
      backend: IndexPackagesService,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): PackageService with GrpcApiService = {
    val service = new ApiPackageService(
      backend = backend
    )
    new PackageServiceValidation(
      service = service,
      ledgerId = ledgerId,
    ) with BindableService {
      override def bindService(): ServerServiceDefinition =
        PackageServiceGrpc.bindService(this, executionContext)
    }
  }
}
