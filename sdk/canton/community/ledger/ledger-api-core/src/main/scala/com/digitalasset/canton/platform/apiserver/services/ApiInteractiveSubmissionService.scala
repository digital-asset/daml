// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v2.interactive.interactive_submission_service.InteractiveSubmissionServiceGrpc.InteractiveSubmissionService as InteractiveSubmissionServiceGrpc
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionRequest,
  ExecuteSubmissionResponse,
  GetPreferredPackageVersionRequest,
  GetPreferredPackageVersionResponse,
  PackagePreference,
  PrepareSubmissionRequest as PrepareRequestP,
  PrepareSubmissionResponse as PrepareResponseP,
}
import com.daml.ledger.api.v2.package_reference.PackageReference
import com.daml.metrics.Timed
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.messages.command.submission.SubmitRequest
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.PrepareRequest
import com.digitalasset.canton.ledger.api.validation.{
  CommandsValidator,
  GetPreferredPackageVersionRequestValidator,
  SubmitRequestValidator,
}
import com.digitalasset.canton.ledger.api.{SubmissionIdGenerator, ValidationLogger}
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

  def prepareWithTraceContext(
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
        .map { case SubmitRequest(commands) =>
          PrepareRequest(commands, request.value.verboseHashing)
        }
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

    val responseFUS = for {
      (parties, packageName, synchronizerIdO, vettingValidAtO) <-
        FutureUnlessShutdown.fromTry(
          GetPreferredPackageVersionRequestValidator.validate(request).toTry
        )
      preferenceO <- interactiveSubmissionService.getPreferredPackageVersion(
        parties = parties,
        packageName = packageName,
        synchronizerId = synchronizerIdO,
        vettingValidAt = vettingValidAtO,
      )
      protoPreference = preferenceO.map { case (packageReference, synchronizerId) =>
        PackagePreference(
          packageReference = Some(
            PackageReference(
              packageId = packageReference.pkdId,
              packageName = packageReference.packageName,
              packageVersion = packageReference.version.toString(),
            )
          ),
          synchronizerId = synchronizerId.toProtoPrimitive,
        )
      }
    } yield GetPreferredPackageVersionResponse(protoPreference)

    responseFUS.asGrpcResponse
  }

  override def close(): Unit = {}

  override def bindService(): ServerServiceDefinition =
    InteractiveSubmissionServiceGrpc.bindService(this, executionContext)
}
