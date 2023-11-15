// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.api.util.{DurationConversion, TimeProvider, TimestampConversion}
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.admin.config_management_service.ConfigManagementServiceGrpc.ConfigManagementService
import com.daml.ledger.api.v1.admin.config_management_service.*
import com.daml.lf.data.{Ref, Time}
import com.daml.logging.LoggingContext
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.{ConfigurationEntry, LedgerOffset}
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.FieldValidator
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.*
import com.digitalasset.canton.ledger.api.{ValidationLogger, domain}
import com.digitalasset.canton.ledger.configuration.{Configuration, LedgerTimeModel}
import com.digitalasset.canton.ledger.error.groups.{AdminServiceErrors, RequestValidationErrors}
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexConfigManagementService
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.LoggingContextUtil.createLoggingContext
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.*
import com.digitalasset.canton.platform.apiserver.services.admin.ApiConfigManagementService.*
import com.digitalasset.canton.platform.apiserver.services.logging
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Failure, Success}

private[apiserver] final class ApiConfigManagementService private (
    index: IndexConfigManagementService,
    writeService: state.WriteConfigService,
    timeProvider: TimeProvider,
    submissionIdGenerator: String => Ref.SubmissionId,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    materializer: Materializer,
    executionContext: ExecutionContext,
) extends ConfigManagementService
    with GrpcApiService
    with NamedLogging {

  private implicit val loggingContext: LoggingContext =
    createLoggingContext(loggerFactory)(identity)

  private val synchronousResponse = new SynchronousResponse(
    new SynchronousResponseStrategy(
      writeService,
      index,
      loggerFactory,
    ),
    loggerFactory,
  )

  override def close(): Unit = synchronousResponse.close()

  override def bindService(): ServerServiceDefinition =
    ConfigManagementServiceGrpc.bindService(this, executionContext)

  override def getTimeModel(request: GetTimeModelRequest): Future[GetTimeModelResponse] = {
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)

    logger.info("Getting time model.")
    index
      .lookupConfiguration()
      .flatMap {
        case Some((_, configuration)) =>
          Future.successful(configurationToResponse(configuration))
        case None =>
          Future.failed(
            RequestValidationErrors.NotFound.LedgerConfiguration
              .Reject()(
                ErrorLoggingContext(
                  logger,
                  loggingContext.toPropertiesMap,
                  loggingContext.traceContext,
                )
              )
              .asGrpcError
          )
      }
      .andThen(logger.logErrorsOnCall[GetTimeModelResponse])
  }

  private def configurationToResponse(configuration: Configuration): GetTimeModelResponse = {
    val timeModel = configuration.timeModel
    GetTimeModelResponse(
      configurationGeneration = configuration.generation,
      timeModel = Some(
        TimeModel(
          avgTransactionLatency = Some(DurationConversion.toProto(timeModel.avgTransactionLatency)),
          minSkew = Some(DurationConversion.toProto(timeModel.minSkew)),
          maxSkew = Some(DurationConversion.toProto(timeModel.maxSkew)),
        )
      ),
    )
  }

  override def setTimeModel(request: SetTimeModelRequest): Future[SetTimeModelResponse] =
    withEnrichedLoggingContext(telemetry)(
      logging.submissionId(request.submissionId)
    ) { implicit loggingContext =>
      logger.info(s"Setting time model, ${loggingContext.serializeFiltered("submissionId")}.")

      implicit val errorLoggingContext: ContextualizedErrorLogger =
        LedgerErrorLoggingContext(
          logger,
          loggingContext.toPropertiesMap,
          loggingContext.traceContext,
          request.submissionId,
        )

      val response = for {
        // Validate and convert the request parameters
        params <- validateParameters(request).fold(
          t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
          Future.successful,
        )

        // Lookup latest configuration to check generation and to extend it with the new time model.
        configuration <- index
          .lookupConfiguration()
          .flatMap {
            case Some(result) =>
              Future.successful(result)
            case None =>
              logger.warn(
                "Could not get the current time model. The index does not yet have any ledger configuration."
              )
              Future.failed(
                RequestValidationErrors.NotFound.LedgerConfiguration
                  .Reject()
                  .asGrpcError
              )
          }
        (ledgerEndBeforeRequest, currentConfig) = configuration

        // Verify that we're modifying the current configuration.
        expectedGeneration = currentConfig.generation
        _ <-
          if (request.configurationGeneration != expectedGeneration) {
            Future.failed(
              ValidationLogger.logFailureWithTrace(
                logger,
                request,
                invalidArgument(
                  s"Mismatching configuration generation, expected $expectedGeneration, received ${request.configurationGeneration}"
                ),
              )
            )
          } else {
            Future.unit
          }

        // Create the new extended configuration.
        newConfig = currentConfig.copy(
          generation = currentConfig.generation + 1,
          timeModel = params.newTimeModel,
        )

        // Submit configuration to the ledger, and start polling for the result.
        augmentedSubmissionId = submissionIdGenerator(request.submissionId)
        entry <- synchronousResponse.submitAndWait(
          augmentedSubmissionId,
          (params.maximumRecordTime, newConfig),
          Some(ledgerEndBeforeRequest),
          params.timeToLive,
        )
      } yield SetTimeModelResponse(entry.configuration.generation)

      response.andThen(logger.logErrorsOnCall[SetTimeModelResponse])
    }

  private case class SetTimeModelParameters(
      newTimeModel: LedgerTimeModel,
      maximumRecordTime: Time.Timestamp,
      timeToLive: FiniteDuration,
  )

  private def validateParameters(
      request: SetTimeModelRequest
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, SetTimeModelParameters] = {
    import FieldValidator.*
    for {
      pTimeModel <- requirePresence(request.newTimeModel, "new_time_model")
      pAvgTransactionLatency <- requirePresence(
        pTimeModel.avgTransactionLatency,
        "avg_transaction_latency",
      )
      pMinSkew <- requirePresence(pTimeModel.minSkew, "min_skew")
      pMaxSkew <- requirePresence(pTimeModel.maxSkew, "max_skew")
      newTimeModel <- LedgerTimeModel(
        avgTransactionLatency = DurationConversion.fromProto(pAvgTransactionLatency),
        minSkew = DurationConversion.fromProto(pMinSkew),
        maxSkew = DurationConversion.fromProto(pMaxSkew),
      ) match {
        case Failure(err) => Left(invalidArgument(err.toString))
        case Success(ok) => Right(ok)
      }
      pMaxRecordTime <- requirePresence(request.maximumRecordTime, "maximum_record_time")
      mrtInstant = TimestampConversion.toInstant(pMaxRecordTime)
      timeToLive = {
        val ttl = java.time.Duration.between(timeProvider.getCurrentTime, mrtInstant)
        if (ttl.isNegative) Duration.Zero
        else Duration.fromNanos(ttl.toNanos)
      }
      maximumRecordTime <- Time.Timestamp
        .fromInstant(mrtInstant)
        .fold(err => Left(invalidArgument(err)), Right(_))
    } yield SetTimeModelParameters(newTimeModel, maximumRecordTime, timeToLive)
  }

}

private[apiserver] object ApiConfigManagementService {

  def createApiService(
      readBackend: IndexConfigManagementService,
      writeBackend: state.WriteConfigService,
      timeProvider: TimeProvider,
      submissionIdGenerator: String => Ref.SubmissionId = augmentSubmissionId,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): ConfigManagementServiceGrpc.ConfigManagementService & GrpcApiService =
    new ApiConfigManagementService(
      readBackend,
      writeBackend,
      timeProvider,
      submissionIdGenerator,
      telemetry,
      loggerFactory,
    )

  private final class SynchronousResponseStrategy(
      writeConfigService: state.WriteConfigService,
      configManagementService: IndexConfigManagementService,
      val loggerFactory: NamedLoggerFactory,
  ) extends SynchronousResponse.Strategy[
        (Time.Timestamp, Configuration),
        ConfigurationEntry,
        ConfigurationEntry.Accepted,
      ]
      with NamedLogging {

    override def submit(
        submissionId: Ref.SubmissionId,
        input: (Time.Timestamp, Configuration),
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[state.SubmissionResult] = {
      val (maximumRecordTime, newConfiguration) = input
      writeConfigService
        .submitConfiguration(maximumRecordTime, submissionId, newConfiguration)
        .asScala
    }

    override def entries(offset: Option[LedgerOffset.Absolute])(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[ConfigurationEntry, _] =
      configManagementService.configurationEntries(offset).map(_._2)

    override def accept(
        submissionId: Ref.SubmissionId
    ): PartialFunction[ConfigurationEntry, ConfigurationEntry.Accepted] = {
      case entry @ domain.ConfigurationEntry.Accepted(`submissionId`, _) =>
        entry
    }

    override def reject(
        submissionId: Ref.SubmissionId
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): PartialFunction[ConfigurationEntry, StatusRuntimeException] = {
      case domain.ConfigurationEntry.Rejected(`submissionId`, reason, _) =>
        AdminServiceErrors.ConfigurationEntryRejected
          .Reject(reason)(
            LedgerErrorLoggingContext(
              logger,
              loggingContext.toPropertiesMap,
              loggingContext.traceContext,
              submissionId,
            )
          )
          .asGrpcError
    }
  }

}
