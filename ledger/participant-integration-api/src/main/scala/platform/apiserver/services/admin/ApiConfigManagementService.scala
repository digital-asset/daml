// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.time.{Duration => JDuration}

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.api.util.{DurationConversion, TimeProvider, TimestampConversion}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{ConfigurationEntry, LedgerOffset}
import com.daml.ledger.api.v1.admin.config_management_service.ConfigManagementServiceGrpc.ConfigManagementService
import com.daml.ledger.api.v1.admin.config_management_service._
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.index.v2.IndexConfigManagementService
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.{Ref, Time}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.admin.ApiConfigManagementService._
import com.daml.platform.apiserver.services.logging
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.server.api.{ValidationLogger, validation}
import com.daml.telemetry.{DefaultTelemetry, TelemetryContext}
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[apiserver] final class ApiConfigManagementService private (
    index: IndexConfigManagementService,
    writeService: state.WriteConfigService,
    timeProvider: TimeProvider,
    submissionIdGenerator: String => Ref.SubmissionId,
)(implicit
    materializer: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends ConfigManagementService
    with GrpcApiService {
  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    ConfigManagementServiceGrpc.bindService(this, executionContext)

  override def getTimeModel(request: GetTimeModelRequest): Future[GetTimeModelResponse] = {
    logger.info("Getting time model")
    index
      .lookupConfiguration()
      .flatMap {
        case Some((_, configuration)) =>
          Future.successful(configurationToResponse(configuration))
        case None =>
          logger.warn(
            "Could not get the current time model. The index does not yet have any ledger configuration."
          )
          Future.failed(ErrorFactories.missingLedgerConfigUponRequest())
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
    withEnrichedLoggingContext(logging.submissionId(request.submissionId)) {
      implicit loggingContext =>
        logger.info("Setting time model")

        implicit val telemetryContext: TelemetryContext =
          DefaultTelemetry.contextFromGrpcThreadLocalContext()

        val response = for {
          // Validate and convert the request parameters
          params <- validateParameters(request).fold(
            t => Future.failed(ValidationLogger.logFailureWithContext(request, t)),
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
                Future.failed(ErrorFactories.missingLedgerConfig(None))
            }
          (ledgerEndBeforeRequest, currentConfig) = configuration

          // Verify that we're modifying the current configuration.
          expectedGeneration = currentConfig.generation
          _ <-
            if (request.configurationGeneration != expectedGeneration) {
              Future.failed(
                ValidationLogger.logFailureWithContext(
                  request,
                  ErrorFactories.invalidArgument(None)(
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
          synchronousResponse = new SynchronousResponse(
            new SynchronousResponseStrategy(
              writeService,
              index,
              ledgerEndBeforeRequest,
            ),
            timeToLive = JDuration.ofMillis(params.timeToLive.toMillis),
          )
          entry <- synchronousResponse.submitAndWait(
            augmentedSubmissionId,
            (params.maximumRecordTime, newConfig),
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
  ): Either[StatusRuntimeException, SetTimeModelParameters] = {
    import validation.FieldValidations._
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
        case Failure(err) => Left(ErrorFactories.invalidArgument(None)(err.toString))
        case Success(ok) => Right(ok)
      }
      // TODO(JM): The maximum record time should be constrained, probably by the current active time model?
      pMaxRecordTime <- requirePresence(request.maximumRecordTime, "maximum_record_time")
      mrtInstant = TimestampConversion.toInstant(pMaxRecordTime)
      timeToLive = {
        val ttl = java.time.Duration.between(timeProvider.getCurrentTime, mrtInstant)
        if (ttl.isNegative) Duration.Zero
        else Duration.fromNanos(ttl.toNanos)
      }
      maximumRecordTime <- Time.Timestamp
        .fromInstant(mrtInstant)
        .fold(err => Left(ErrorFactories.invalidArgument(None)(err)), Right(_))
    } yield SetTimeModelParameters(newTimeModel, maximumRecordTime, timeToLive)
  }

}

private[apiserver] object ApiConfigManagementService {

  def createApiService(
      readBackend: IndexConfigManagementService,
      writeBackend: state.WriteConfigService,
      timeProvider: TimeProvider,
      submissionIdGenerator: String => Ref.SubmissionId = augmentSubmissionId,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ConfigManagementServiceGrpc.ConfigManagementService with GrpcApiService =
    new ApiConfigManagementService(
      readBackend,
      writeBackend,
      timeProvider,
      submissionIdGenerator,
    )

  private final class SynchronousResponseStrategy(
      writeConfigService: state.WriteConfigService,
      configManagementService: IndexConfigManagementService,
      ledgerEnd: LedgerOffset.Absolute,
  )(implicit loggingContext: LoggingContext)
      extends SynchronousResponse.Strategy[
        (Time.Timestamp, Configuration),
        ConfigurationEntry,
        ConfigurationEntry.Accepted,
      ] {

    override def currentLedgerEnd(): Future[Option[LedgerOffset.Absolute]] =
      Future.successful(Some(ledgerEnd))

    override def submit(
        submissionId: Ref.SubmissionId,
        input: (Time.Timestamp, Configuration),
    )(implicit telemetryContext: TelemetryContext): Future[state.SubmissionResult] = {
      val (maximumRecordTime, newConfiguration) = input
      writeConfigService
        .submitConfiguration(maximumRecordTime, submissionId, newConfiguration)
        .toScala
    }

    override def entries(offset: Option[LedgerOffset.Absolute]): Source[ConfigurationEntry, _] =
      configManagementService.configurationEntries(offset).map(_._2)

    override def accept(
        submissionId: Ref.SubmissionId
    ): PartialFunction[ConfigurationEntry, ConfigurationEntry.Accepted] = {
      case entry @ domain.ConfigurationEntry.Accepted(`submissionId`, _) =>
        entry
    }

    override def reject(
        submissionId: Ref.SubmissionId
    ): PartialFunction[ConfigurationEntry, StatusRuntimeException] = {
      case domain.ConfigurationEntry.Rejected(`submissionId`, reason, _) =>
        ErrorFactories.aborted(reason, None)
    }
  }

}
