// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v2.interactive.interactive_submission_service.InteractiveSubmissionServiceGrpc.InteractiveSubmissionService as InteractiveSubmissionServiceGrpc
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
  PackagePreference,
  PackageVettingRequirement,
  PrepareSubmissionRequest as PrepareRequestP,
  PrepareSubmissionResponse as PrepareResponseP,
}
import com.daml.ledger.api.v2.package_reference.PackageReference
import com.daml.metrics.Timed
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.ExecuteRequest
import com.digitalasset.canton.ledger.api.validation.{
  CommandsValidator,
  GetPreferredPackagesRequestValidator,
  SubmitRequestValidator,
}
import com.digitalasset.canton.ledger.api.{SubmissionIdGenerator, ValidationLogger}
import com.digitalasset.canton.ledger.error.LedgerApiErrors.NoPreferredPackagesFound
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.TimerAndTrackOnShutdownSyntax
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcFUSExtended
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.OptionUtil
import io.grpc.ServerServiceDefinition
import io.scalaland.chimney.auto.*
import io.scalaland.chimney.syntax.*

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}

class ApiInteractiveSubmissionService(
    interactiveSubmissionService: InteractiveSubmissionService & AutoCloseable,
    commandsValidator: CommandsValidator,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationDuration: Duration,
    submissionIdGenerator: SubmissionIdGenerator,
    metrics: LedgerApiServerMetrics,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends InteractiveSubmissionServiceGrpc
    with GrpcApiService
    with NamedLogging {

  private val validator = new SubmitRequestValidator(commandsValidator)

  override def prepareSubmission(request: PrepareRequestP): Future[PrepareResponseP] = {
    implicit val traceContext = getPrepareRequestTraceContext(
      request.userId,
      request.commandId,
      request.actAs,
      telemetry,
    )
    prepareWithTraceContext(Traced(request)).asGrpcResponse
  }

  private def prepareWithTraceContext(
      request: Traced[PrepareRequestP]
  ): FutureUnlessShutdown[PrepareResponseP] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(request.traceContext)
    val errorLogger: ErrorLoggingContext =
      ErrorLoggingContext.fromOption(
        logger,
        loggingContextWithTrace,
        None,
      )

    Timed.timedAndTrackedFutureUS(
      metrics.commands.interactivePrepares,
      metrics.commands.preparesRunning,
      Timed
        .value(
          metrics.commands.validation,
          validator.validatePrepare(
            req = request.value,
            currentLedgerTime = currentLedgerTime(),
            currentUtcTime = currentUtcTime(),
            maxDeduplicationDuration = maxDeduplicationDuration,
          )(errorLogger),
        )
        .fold(
          t =>
            FutureUnlessShutdown.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
          interactiveSubmissionService.prepare(_),
        ),
    )
  }

  override def executeSubmission(
      request: ExecuteSubmissionRequest
  ): Future[ExecuteSubmissionResponse] = {
    val submitterInfo = request.preparedTransaction.flatMap(_.metadata.flatMap(_.submitterInfo))
    implicit val traceContext: TraceContext = getExecuteRequestTraceContext(
      request.userId,
      submitterInfo.map(_.commandId),
      submitterInfo.map(_.actAs).toList.flatten,
      telemetry,
    )
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)
    val errorLogger: ErrorLoggingContext =
      ErrorLoggingContext.fromOption(
        logger,
        loggingContextWithTrace,
        OptionUtil.emptyStringAsNone(request.submissionId),
      )
    validator
      .validateExecute(
        request,
        currentLedgerTime(),
        submissionIdGenerator,
        maxDeduplicationDuration,
      )(errorLogger)
      .fold(
        t => FutureUnlessShutdown.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        interactiveSubmissionService.execute(_),
      )
      .asGrpcResponse
  }

  override def getPreferredPackageVersion(
      request: GetPreferredPackageVersionRequest
  ): Future[GetPreferredPackageVersionResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)

    implicit val traceContext: TraceContext = loggingContextWithTrace.traceContext

    getPreferredPackagesInternal(
      GetPreferredPackagesRequest(
        packageVettingRequirements =
          Seq(PackageVettingRequirement(request.parties, request.packageName)),
        synchronizerId = request.synchronizerId,
        vettingValidAt = request.vettingValidAt,
      )
    ).map {
      case Right((Seq(packageReference), synchronizerId)) =>
        GetPreferredPackageVersionResponse(
          packagePreference = Some(PackagePreference(Some(packageReference), synchronizerId))
        )
      case Right((unexpectedPackageReferences, _)) =>
        throw new RuntimeException(
          s"Expected exactly one package reference but got: $unexpectedPackageReferences"
        )
      case Left(failureReason) =>
        logger.debug(s"Could not compute the preferred package versions: $failureReason")
        GetPreferredPackageVersionResponse(packagePreference = None)
    }
  }

  override def getPreferredPackages(
      request: GetPreferredPackagesRequest
  ): Future[GetPreferredPackagesResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)

    implicit val traceContext: TraceContext = loggingContextWithTrace.traceContext

    for {
      preferencesEUS <- getPreferredPackagesInternal(request)
      response <- preferencesEUS.fold(
        preferenceSelectionFailed =>
          Future.failed(NoPreferredPackagesFound.Reject(preferenceSelectionFailed).asGrpcError),
        { case (packageReferences, synchronizerId) =>
          Future.successful(GetPreferredPackagesResponse(packageReferences, synchronizerId))
        },
      )
    } yield response
  }

  private def getPreferredPackagesInternal(
      request: GetPreferredPackagesRequest
  ): Future[Either[String, (Seq[PackageReference], String)]] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)

    implicit val traceContext: TraceContext = loggingContextWithTrace.traceContext

    GetPreferredPackagesRequestValidator
      .validate(request)
      .fold(
        t => FutureUnlessShutdown.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        { case (packageVettingRequirements, synchronizerIdO, vettingValidAtO) =>
          interactiveSubmissionService.getPreferredPackages(
            packageVettingRequirements = packageVettingRequirements,
            synchronizerId = synchronizerIdO,
            vettingValidAt = vettingValidAtO,
          )
        },
      )
      .map(_.map { case (domainPackageReferences, synchronizerId) =>
        (
          domainPackageReferences.map(packageReference =>
            PackageReference(
              packageId = packageReference.pkgId,
              packageName = packageReference.packageName,
              packageVersion = packageReference.version.toString(),
            )
          ),
          synchronizerId.logical.toProtoPrimitive,
        )
      })
      .asGrpcResponse
  }

  override def close(): Unit = {}

  override def bindService(): ServerServiceDefinition =
    InteractiveSubmissionServiceGrpc.bindService(this, executionContext)

  private def executeAndWaitInternal[A](
      request: ExecuteSubmissionRequest,
      execute: (ExecuteRequest, LoggingContextWithTrace) => FutureUnlessShutdown[A],
  ) = {
    val submitterInfo = request.preparedTransaction.flatMap(_.metadata.flatMap(_.submitterInfo))
    implicit val traceContext: TraceContext = getExecuteRequestTraceContext(
      request.userId,
      submitterInfo.map(_.commandId),
      submitterInfo.map(_.actAs).toList.flatten,
      telemetry,
    )

    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)

    val errorLogger: ErrorLoggingContext =
      ErrorLoggingContext.fromOption(
        logger,
        loggingContextWithTrace,
        OptionUtil.emptyStringAsNone(request.submissionId),
      )
    validator
      .validateExecute(
        request.transformInto[ExecuteSubmissionRequest],
        currentLedgerTime(),
        submissionIdGenerator,
        maxDeduplicationDuration,
      )(errorLogger)
      .fold(
        t => FutureUnlessShutdown.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        execute(_, loggingContextWithTrace),
      )
      .asGrpcResponse
  }

  override def executeSubmissionAndWait(
      request: ExecuteSubmissionAndWaitRequest
  ): Future[ExecuteSubmissionAndWaitResponse] =
    executeAndWaitInternal(
      // Convert the ExecuteSubmissionAndWaitRequest request into an ExecuteSubmissionRequest
      // They are duplicated for better UX on the API but their fields are identical
      request.transformInto[ExecuteSubmissionRequest],
      (executeRequest, loggingContext) =>
        interactiveSubmissionService.executeAndWait(executeRequest)(loggingContext),
    )

  override def executeSubmissionAndWaitForTransaction(
      request: ExecuteSubmissionAndWaitForTransactionRequest
  ): Future[ExecuteSubmissionAndWaitForTransactionResponse] =
    executeAndWaitInternal(
      request.transformInto[ExecuteSubmissionRequest],
      (executeRequest, loggingContext) =>
        interactiveSubmissionService.executeAndWaitForTransaction(
          executeRequest,
          ApiCommandService.generateTransactionFormatIfEmpty(
            executeRequest.preparedTransaction.metadata.toList
              .flatMap(_.submitterInfo)
              .flatMap(_.actAs)
          )(request.transactionFormat),
        )(loggingContext),
    )
}
