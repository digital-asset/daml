// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.error.DamlError
import com.daml.ledger.api.v2.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v2.admin.package_management_service.*
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.ledger.participant.state.{SubmissionResult, WriteService}
import com.digitalasset.canton.logging.LoggingContextUtil.createLoggingContext
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.logging
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

private[apiserver] final class ApiPackageManagementService private (
    writeService: WriteService,
    submissionIdGenerator: String => Ref.SubmissionId,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends PackageManagementService
    with GrpcApiService
    with NamedLogging {

  private implicit val loggingContext: LoggingContext =
    createLoggingContext(loggerFactory)(identity)

  override def close(): Unit = {
    // Nothing to do in this service's close.
    // All backend operations are guarded
  }

  override def bindService(): ServerServiceDefinition =
    PackageManagementServiceGrpc.bindService(this, executionContext)

  override def listKnownPackages(
      request: ListKnownPackagesRequest
  ): Future[ListKnownPackagesResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)

    logger.info("Listing known packages.")
    writeService
      .listLfPackages()
      .map { pkgs =>
        ListKnownPackagesResponse(pkgs.map { pkgDescription =>
          PackageDetails(
            pkgDescription.packageId.toString,
            pkgDescription.packageSize.toLong,
            Some(TimestampConversion.fromLf(pkgDescription.uploadedAt.underlying)),
            pkgDescription.sourceDescription.toString,
          )
        })
      }
      .andThen(logger.logErrorsOnCall[ListKnownPackagesResponse])
  }

  override def validateDarFile(request: ValidateDarFileRequest): Future[ValidateDarFileResponse] = {
    LoggingContextWithTrace.withEnrichedLoggingContext(telemetry)(
      logging.submissionId(submissionIdGenerator(request.submissionId))
    ) { implicit loggingContext: LoggingContextWithTrace =>
      logger.info(s"Validating DAR file, ${loggingContext.serializeFiltered("submissionId")}.")
      writeService
        .validateDar(dar = request.darFile, darName = "defaultDarName")
        .flatMap {
          case SubmissionResult.Acknowledged => Future.successful(ValidateDarFileResponse())
          case err: SubmissionResult.SynchronousError => Future.failed(err.exception)
        }
    }
  }

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] = {
    val submissionId = submissionIdGenerator(request.submissionId)
    LoggingContextWithTrace.withEnrichedLoggingContext(telemetry)(
      logging.submissionId(submissionId)
    ) { implicit loggingContext: LoggingContextWithTrace =>
      logger.info(s"Uploading DAR file, ${loggingContext.serializeFiltered("submissionId")}.")

      writeService
        .uploadDar(request.darFile, submissionId)
        .flatMap {
          case SubmissionResult.Acknowledged => Future.successful(UploadDarFileResponse())
          case err: SubmissionResult.SynchronousError => Future.failed(err.exception)
        }
        .andThen(logger.logErrorsOnCall[UploadDarFileResponse])
    }
  }
}

private[apiserver] object ApiPackageManagementService {

  def createApiService(
      writeService: WriteService,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): PackageManagementServiceGrpc.PackageManagementService & GrpcApiService =
    new ApiPackageManagementService(
      writeService,
      augmentSubmissionId,
      telemetry,
      loggerFactory,
    )

  implicit class ErrorValidations[E, R](result: Either[E, R]) {
    def handleError(toSelfServiceErrorCode: E => DamlError): Try[R] =
      result.left.map { err =>
        toSelfServiceErrorCode(err).asGrpcError
      }.toTry
  }
}
