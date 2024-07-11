// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.daml_lf_dev.DamlLf.{Archive, HashFunction}
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.package_service.PackageServiceGrpc.PackageService
import com.daml.ledger.api.v2.package_service.{
  GetPackageRequest,
  GetPackageResponse,
  GetPackageStatusRequest,
  GetPackageStatusResponse,
  HashFunction as APIHashFunction,
  ListPackagesRequest,
  ListPackagesResponse,
  PackageServiceGrpc,
  PackageStatus,
}
import com.daml.logging.LoggingContext
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.grpc.Logging.traceId
import com.digitalasset.canton.ledger.api.validation.ValidationErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state.WriteService
import com.digitalasset.canton.logging.LoggingContextUtil.createLoggingContext
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.daml.lf.data.Ref
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiPackageService(
    writeService: WriteService,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends PackageService
    with GrpcApiService
    with NamedLogging {
  private implicit val loggingContext: LoggingContext =
    createLoggingContext(loggerFactory)(identity)

  override def bindService(): ServerServiceDefinition =
    PackageServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    logger.info(s"Received request to list packages: $request")
    writeService
      .listLfPackages()
      .map(p => ListPackagesResponse(p.map(_.packageId.toString)))
      .andThen(logger.logErrorsOnCall[ListPackagesResponse])
  }

  override def getPackage(request: GetPackageRequest): Future[GetPackageResponse] =
    withEnrichedLoggingContext(telemetry)(
      logging.packageId(request.packageId),
      traceId(telemetry.traceIdFromGrpcContext),
    ) { implicit loggingContext =>
      logger.info(s"Received request for a package: $request")
      withValidatedPackageId(request.packageId, request) { packageId =>
        writeService
          .getLfArchive(packageId)
          .flatMap {
            case None =>
              Future.failed[GetPackageResponse](
                RequestValidationErrors.NotFound.Package
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
    LoggingContextWithTrace.withEnrichedLoggingContext(telemetry)(
      logging.packageId(request.packageId)
    ) { implicit loggingContext =>
      logger.info(s"Received request for a package status: $request")
      withValidatedPackageId(request.packageId, request) { packageId =>
        Future {
          val result =
            if (
              writeService.getPackageMetadataSnapshot.packageIdVersionMap.keySet.contains(packageId)
            ) {
              PackageStatus.PACKAGE_STATUS_REGISTERED
            } else {
              PackageStatus.PACKAGE_STATUS_UNSPECIFIED
            }
          GetPackageStatusResponse(result)
        }
          .andThen(logger.logErrorsOnCall[GetPackageStatusResponse])
      }
    }

  private def withValidatedPackageId[T, R](packageId: String, request: R)(
      block: Ref.PackageId => Future[T]
  )(implicit loggingContext: LoggingContextWithTrace): Future[T] =
    Ref.PackageId
      .fromString(packageId)
      .fold(
        errorMessage =>
          Future.failed[T](
            ValidationLogger.logFailureWithTrace(
              logger,
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
      case HashFunction.SHA256 => APIHashFunction.HASH_FUNCTION_SHA256
      case _ => APIHashFunction.Unrecognized(-1)
    }
    GetPackageResponse(
      hashFunction = hashFunction,
      archivePayload = archive.getPayload,
      hash = archive.getHash,
    )
  }

  private def createContextualizedErrorLogger(implicit
      loggingContext: LoggingContextWithTrace
  ): ContextualizedErrorLogger =
    ErrorLoggingContext(logger, loggingContext)
}
