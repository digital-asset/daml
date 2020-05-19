// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.index.v2.IndexConfigManagementService
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{
  Configuration,
  SubmissionId,
  SubmissionResult,
  WriteConfigService
}
import com.daml.api.util.{DurationConversion, TimeProvider, TimestampConversion}
import com.daml.lf.data.Time
import com.daml.dec.{DirectExecutionContext => DE}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.api.v1.admin.config_management_service.ConfigManagementServiceGrpc.ConfigManagementService
import com.daml.ledger.api.v1.admin.config_management_service._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.platform.server.api.validation
import com.daml.platform.server.api.validation.ErrorFactories
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.{Failure, Success}

final class ApiConfigManagementService private (
    index: IndexConfigManagementService,
    writeService: WriteConfigService,
    timeProvider: TimeProvider,
    ledgerConfiguration: LedgerConfiguration,
    materializer: Materializer
)(implicit logCtx: LoggingContext)
    extends ConfigManagementService
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val defaultConfigResponse = configToResponse(ledgerConfiguration.initialConfiguration)

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    ConfigManagementServiceGrpc.bindService(this, DE)

  override def getTimeModel(request: GetTimeModelRequest): Future[GetTimeModelResponse] =
    index
      .lookupConfiguration()
      .map(_.fold(defaultConfigResponse) { case (_, conf) => configToResponse(conf) })(DE)
      .andThen(logger.logErrorsOnCall[GetTimeModelResponse])(DE)

  private def configToResponse(config: Configuration): GetTimeModelResponse = {
    val tm = config.timeModel
    GetTimeModelResponse(
      configurationGeneration = config.generation,
      timeModel = Some(
        TimeModel(
          avgTransactionLatency = Some(DurationConversion.toProto(tm.avgTransactionLatency)),
          minSkew = Some(DurationConversion.toProto(tm.minSkew)),
          maxSkew = Some(DurationConversion.toProto(tm.maxSkew))
        ))
    )
  }

  override def setTimeModel(request: SetTimeModelRequest): Future[SetTimeModelResponse] = {
    // Execute subsequent transforms in the thread of the previous
    // operation.
    implicit val ec: ExecutionContext = DE

    val response = for {
      // Validate and convert the request parameters
      params <- validateParameters(request).fold(Future.failed(_), Future.successful)

      // Lookup latest configuration to check generation and to extend it with the new time model.
      optConfigAndOffset <- index.lookupConfiguration()
      pollOffset = optConfigAndOffset.map(_._1)
      currentConfig = optConfigAndOffset
        .map(_._2)
        .getOrElse(ledgerConfiguration.initialConfiguration)

      // Verify that we're modifying the current configuration.
      _ <- if (request.configurationGeneration != currentConfig.generation) {
        Future.failed(ErrorFactories.invalidArgument(
          s"Mismatching configuration generation, expected ${currentConfig.generation}, received ${request.configurationGeneration}"))
      } else {
        Future.successful(())
      }

      // Create the new extended configuration.
      newConfig = currentConfig.copy(
        generation = currentConfig.generation + 1,
        timeModel = params.newTimeModel
      )

      // Submit configuration to the ledger, and start polling for the result.
      submissionResult <- FutureConverters
        .toScala(
          writeService.submitConfiguration(
            params.maximumRecordTime,
            SubmissionId.assertFromString(request.submissionId),
            newConfig
          ))

      result <- submissionResult match {
        case SubmissionResult.Acknowledged =>
          // Ledger acknowledged. Start polling to wait for the result to land in the index.
          pollUntilPersisted(request.submissionId, pollOffset, params.timeToLive).flatMap {
            case accept: domain.ConfigurationEntry.Accepted =>
              Future.successful(SetTimeModelResponse(accept.configuration.generation))
            case rejected: domain.ConfigurationEntry.Rejected =>
              Future.failed(ErrorFactories.aborted(rejected.rejectionReason))
          }
        case SubmissionResult.Overloaded =>
          Future.failed(ErrorFactories.resourceExhausted("Resource exhausted"))
        case SubmissionResult.InternalError(reason) =>
          Future.failed(ErrorFactories.internal(reason))
        case SubmissionResult.NotSupported =>
          Future.failed(
            ErrorFactories.unimplemented("Setting of time model not supported by this ledger"))
      }
    } yield result

    response.andThen(logger.logErrorsOnCall[SetTimeModelResponse])
  }

  private case class SetTimeModelParameters(
      newTimeModel: v1.TimeModel,
      maximumRecordTime: Time.Timestamp,
      timeToLive: FiniteDuration
  )

  private def validateParameters(
      request: SetTimeModelRequest): Either[StatusRuntimeException, SetTimeModelParameters] = {
    import validation.FieldValidations._
    for {
      pTimeModel <- requirePresence(request.newTimeModel, "new_time_model")
      pAvgTransactionLatency <- requirePresence(
        pTimeModel.avgTransactionLatency,
        "avg_transaction_latency")
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

  private def pollUntilPersisted(
      submissionId: String,
      offset: Option[LedgerOffset.Absolute],
      timeToLive: FiniteDuration): Future[domain.ConfigurationEntry] = {
    index
      .configurationEntries(offset)
      .collect {
        case (_, entry @ domain.ConfigurationEntry.Accepted(`submissionId`, _, _)) => entry
        case (_, entry @ domain.ConfigurationEntry.Rejected(`submissionId`, _, _, _)) => entry
      }
      .completionTimeout(timeToLive)
      .runWith(Sink.head)(materializer)
      .recoverWith {
        case _: TimeoutException =>
          Future.failed(ErrorFactories.aborted("Request timed out"))
      }(DE)
  }

}

object ApiConfigManagementService {
  def createApiService(
      readBackend: IndexConfigManagementService,
      writeBackend: WriteConfigService,
      timeProvider: TimeProvider,
      ledgerConfiguration: LedgerConfiguration)(implicit mat: Materializer, logCtx: LoggingContext)
    : ConfigManagementServiceGrpc.ConfigManagementService with GrpcApiService =
    new ApiConfigManagementService(
      readBackend,
      writeBackend,
      timeProvider,
      ledgerConfiguration,
      mat)

}
