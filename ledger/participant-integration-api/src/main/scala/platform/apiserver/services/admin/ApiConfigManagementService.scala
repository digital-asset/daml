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
import com.daml.ledger.participant.state.index.v2.IndexConfigManagementService
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{
  Configuration,
  SubmissionId,
  SubmissionResult,
  WriteConfigService,
}
import com.daml.lf.data.Time
import com.daml.platform.apiserver.services.logging
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.admin.ApiConfigManagementService._
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.platform.server.api.validation
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[apiserver] final class ApiConfigManagementService private (
    index: IndexConfigManagementService,
    writeService: WriteConfigService,
    timeProvider: TimeProvider,
    ledgerConfiguration: LedgerConfiguration,
)(implicit
    materializer: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends ConfigManagementService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val defaultConfigResponse = configToResponse(
    ledgerConfiguration.initialConfiguration.copy(generation = LedgerConfiguration.NoGeneration)
  )

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    ConfigManagementServiceGrpc.bindService(this, executionContext)

  override def getTimeModel(request: GetTimeModelRequest): Future[GetTimeModelResponse] = {
    logger.info("Getting time model")
    index
      .lookupConfiguration()
      .map(_.fold(defaultConfigResponse) { case (_, conf) => configToResponse(conf) })
      .andThen(logger.logErrorsOnCall[GetTimeModelResponse])
  }

  private def configToResponse(config: Configuration): GetTimeModelResponse = {
    val tm = config.timeModel
    GetTimeModelResponse(
      configurationGeneration = config.generation,
      timeModel = Some(
        TimeModel(
          avgTransactionLatency = Some(DurationConversion.toProto(tm.avgTransactionLatency)),
          minSkew = Some(DurationConversion.toProto(tm.minSkew)),
          maxSkew = Some(DurationConversion.toProto(tm.maxSkew)),
        )
      ),
    )
  }

  override def setTimeModel(request: SetTimeModelRequest): Future[SetTimeModelResponse] =
    withEnrichedLoggingContext(logging.submissionId(request.submissionId)) {
      implicit loggingContext =>
        logger.info("Setting time model")
        val response = for {
          // Validate and convert the request parameters
          params <- validateParameters(request).fold(Future.failed(_), Future.successful)

          // Lookup latest configuration to check generation and to extend it with the new time model.
          optConfigAndOffset <- index.lookupConfiguration()
          ledgerEndBeforeRequest = optConfigAndOffset.map(_._1)
          currentConfig = optConfigAndOffset.map(_._2)

          // Verify that we're modifying the current configuration.
          expectedGeneration = currentConfig
            .map(_.generation)
            .getOrElse(LedgerConfiguration.NoGeneration)
          _ <-
            if (request.configurationGeneration != expectedGeneration) {
              Future.failed(
                ErrorFactories.invalidArgument(
                  s"Mismatching configuration generation, expected $expectedGeneration, received ${request.configurationGeneration}"
                )
              )
            } else {
              Future.unit
            }

          // Create the new extended configuration.
          newConfig = currentConfig
            .map(config => config.copy(generation = config.generation + 1))
            .getOrElse(ledgerConfiguration.initialConfiguration)
            .copy(timeModel = params.newTimeModel)

          // Submit configuration to the ledger, and start polling for the result.
          submissionId = SubmissionId.assertFromString(request.submissionId)
          synchronousResponse = new SynchronousResponse(
            new SynchronousResponseStrategy(
              writeService,
              index,
              ledgerEndBeforeRequest,
            ),
            timeToLive = JDuration.ofMillis(params.timeToLive.toMillis),
          )
          entry <- synchronousResponse.submitAndWait(
            submissionId,
            (params.maximumRecordTime, newConfig),
          )(executionContext, materializer)
        } yield SetTimeModelResponse(entry.configuration.generation)

        response.andThen(logger.logErrorsOnCall[SetTimeModelResponse])
    }

  private case class SetTimeModelParameters(
      newTimeModel: v1.TimeModel,
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
      newTimeModel <- v1.TimeModel(
        avgTransactionLatency = DurationConversion.fromProto(pAvgTransactionLatency),
        minSkew = DurationConversion.fromProto(pMinSkew),
        maxSkew = DurationConversion.fromProto(pMaxSkew),
      ) match {
        case Failure(err) => Left(ErrorFactories.invalidArgument(err.toString))
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
        .fold(err => Left(ErrorFactories.invalidArgument(err)), Right(_))
    } yield SetTimeModelParameters(newTimeModel, maximumRecordTime, timeToLive)
  }

}

private[apiserver] object ApiConfigManagementService {

  def createApiService(
      readBackend: IndexConfigManagementService,
      writeBackend: WriteConfigService,
      timeProvider: TimeProvider,
      ledgerConfiguration: LedgerConfiguration,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ConfigManagementServiceGrpc.ConfigManagementService with GrpcApiService =
    new ApiConfigManagementService(
      readBackend,
      writeBackend,
      timeProvider,
      ledgerConfiguration,
    )

  private final class SynchronousResponseStrategy(
      writeConfigService: WriteConfigService,
      configManagementService: IndexConfigManagementService,
      ledgerEnd: Option[LedgerOffset.Absolute],
  )(implicit loggingContext: LoggingContext)
      extends SynchronousResponse.Strategy[
        (Time.Timestamp, Configuration),
        ConfigurationEntry,
        ConfigurationEntry.Accepted,
      ] {

    override def currentLedgerEnd(): Future[Option[LedgerOffset.Absolute]] =
      Future.successful(ledgerEnd)

    override def submit(
        submissionId: SubmissionId,
        input: (Time.Timestamp, Configuration),
    ): Future[SubmissionResult] = {
      val (maximumRecordTime, newConfiguration) = input
      writeConfigService
        .submitConfiguration(maximumRecordTime, submissionId, newConfiguration)
        .toScala
    }

    override def entries(offset: Option[LedgerOffset.Absolute]): Source[ConfigurationEntry, _] =
      configManagementService.configurationEntries(offset).map(_._2)

    override def accept(
        submissionId: SubmissionId
    ): PartialFunction[ConfigurationEntry, ConfigurationEntry.Accepted] = {
      case entry @ domain.ConfigurationEntry.Accepted(`submissionId`, _) =>
        entry
    }

    override def reject(
        submissionId: SubmissionId
    ): PartialFunction[ConfigurationEntry, StatusRuntimeException] = {
      case domain.ConfigurationEntry.Rejected(`submissionId`, reason, _) =>
        ErrorFactories.aborted(reason)
    }
  }

}
