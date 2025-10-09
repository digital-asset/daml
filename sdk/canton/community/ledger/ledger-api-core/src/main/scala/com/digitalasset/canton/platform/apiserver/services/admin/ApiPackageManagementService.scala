// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import cats.data.EitherT
import cats.implicits.{toBifunctorOps, toTraverseOps}
import com.daml.ledger.api.v2.admin.package_management_service.*
import com.daml.ledger.api.v2.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.daml.ledger.api.v2.package_reference.VettedPackages
import com.daml.logging.LoggingContext
import com.daml.tracing.Telemetry
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.ledger.api.{
  PriorTopologySerialNone,
  UpdateVettedPackagesOpts,
  UploadDarVettingChange as UploadDarOpts,
}
import com.digitalasset.canton.ledger.participant.state.{PackageSyncService, SubmissionResult}
import com.digitalasset.canton.logging.LoggingContextUtil.createLoggingContext
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.platform.apiserver.services.logging
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.EitherUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{EitherTUtil, OptionUtil}
import com.digitalasset.daml.lf.data.Ref
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

private[apiserver] final class ApiPackageManagementService private (
    packageSyncService: PackageSyncService,
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
    packageSyncService
      .listLfPackages()
      .map { pkgs =>
        ListKnownPackagesResponse(pkgs.map { pkgDescription =>
          PackageDetails(
            pkgDescription.packageId,
            pkgDescription.packageSize.toLong,
            Some(TimestampConversion.fromLf(pkgDescription.uploadedAt.underlying)),
            name = pkgDescription.name.unwrap,
            version = pkgDescription.version.unwrap,
          )
        })
      }
      .thereafter(logger.logErrorsOnCall[ListKnownPackagesResponse])
  }

  override def validateDarFile(request: ValidateDarFileRequest): Future[ValidateDarFileResponse] =
    LoggingContextWithTrace.withEnrichedLoggingContext(telemetry)(
      logging.submissionId(submissionIdGenerator(request.submissionId))
    ) { implicit loggingContext: LoggingContextWithTrace =>
      logger.info(s"Validating DAR file, ${loggingContext.serializeFiltered("submissionId")}.")
      for {
        synchronizerIdO <-
          EitherTUtil.toFuture(
            CantonGrpcUtil.mapErrNew(
              OptionUtil
                .emptyStringAsNone(request.synchronizerId)
                .traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"))
                .leftMap(ProtoDeserializationFailure.Wrap(_))
            )
          )
        result <- packageSyncService
          .validateDar(
            dar = request.darFile,
            darName = "defaultDarName",
            synchronizerId = synchronizerIdO,
          )
          .flatMap {
            case SubmissionResult.Acknowledged => Future.successful(ValidateDarFileResponse())
            case err: SubmissionResult.SynchronousError => Future.failed(err.exception)
          }
      } yield result
    }

  override def uploadDarFile(request: UploadDarFileRequest): Future[UploadDarFileResponse] = {
    val submissionId = submissionIdGenerator(request.submissionId)
    LoggingContextWithTrace.withEnrichedLoggingContext(telemetry)(
      logging.submissionId(submissionId)
    ) { implicit loggingContext: LoggingContextWithTrace =>
      logger.info(s"Uploading DAR file, ${loggingContext.serializeFiltered("submissionId")}.")

      val resultET = for {
        synchronizerIdO <-
          CantonGrpcUtil.mapErrNew(
            OptionUtil
              .emptyStringAsNone(request.synchronizerId)
              .traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"))
              .leftMap(ProtoDeserializationFailure.Wrap(_))
          )
        uploadDarVettingChange <- CantonGrpcUtil
          .mapErrNew(
            UploadDarOpts
              .fromProto("vetting_change", request.vettingChange)
              .leftMap(ProtoDeserializationFailure.Wrap(_))
          )
        uploadResult <- EitherT.right(
          packageSyncService
            .uploadDar(Seq(request.darFile), submissionId, uploadDarVettingChange, synchronizerIdO)
        )
        response <- uploadResult match {
          case SubmissionResult.Acknowledged =>
            EitherT.rightT[Future, StatusRuntimeException](UploadDarFileResponse())
          case err: SubmissionResult.SynchronousError =>
            EitherT.leftT[Future, UploadDarFileResponse](err.exception)
        }
      } yield response
      EitherTUtil.toFuture(resultET).thereafter(logger.logErrorsOnCall[UploadDarFileResponse])
    }
  }

  override def updateVettedPackages(
      request: UpdateVettedPackagesRequest
  ): Future[UpdateVettedPackagesResponse] = {
    val submissionId = submissionIdGenerator("")
    LoggingContextWithTrace.withEnrichedLoggingContext(telemetry)(
      logging.submissionId(submissionId)
    ) { implicit loggingContext: LoggingContextWithTrace =>
      for {
        updateVettedPackagesOpts <- UpdateVettedPackagesOpts
          .fromProto(request)
          .toFuture(ProtoDeserializationFailure.Wrap(_).asGrpcError)
        result <- packageSyncService.updateVettedPackages(updateVettedPackagesOpts)
      } yield result match {
        case (previousStates, newStates) =>
          UpdateVettedPackagesResponse(
            // TODO(#27750) Make sure to only populate this when a prior vetting
            // state actually exists. If no vetting state exists, this should be
            // None.
            pastVettedPackages = Some(
              VettedPackages(
                packages = previousStates.map(_.toProtoLAPI),
                // TODO(#27750) Populate these fields and assert over them when
                // updates and queries can specify target synchronizers
                participantId = "",
                synchronizerId = "",
                topologySerial = Some(PriorTopologySerialNone.toProtoLAPI),
              )
            ),
            newVettedPackages = Some(
              VettedPackages(
                packages = newStates.map(_.toProtoLAPI),
                // TODO(#27750) Populate these fields and assert over them when
                // updates and queries can specify target synchronizers
                participantId = "",
                synchronizerId = "",
                topologySerial = Some(PriorTopologySerialNone.toProtoLAPI),
              )
            ),
          )
      }
    }
  }
}

private[apiserver] object ApiPackageManagementService {

  def createApiService(
      packageSyncService: PackageSyncService,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): PackageManagementServiceGrpc.PackageManagementService & GrpcApiService =
    new ApiPackageManagementService(
      packageSyncService,
      augmentSubmissionId,
      telemetry,
      loggerFactory,
    )

  implicit class ErrorValidations[E, R](result: Either[E, R]) {
    def handleError(toSelfServiceErrorCode: E => RpcError): Try[R] =
      result.left.map { err =>
        toSelfServiceErrorCode(err).asGrpcError
      }.toTry
  }
}
