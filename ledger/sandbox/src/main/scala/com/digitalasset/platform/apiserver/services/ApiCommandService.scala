// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.Instant

import akka.NotUsed
import akka.actor.Cancellable
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Source}
import com.daml.api.util.TimeProvider
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionEndResponse,
  CompletionStreamRequest,
  CompletionStreamResponse
}
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionByIdRequest,
  GetTransactionResponse
}
import com.daml.ledger.client.services.commands.{CommandCompletionSource, CommandTrackerFlow}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.ApiCommandService._
import com.daml.platform.apiserver.services.tracking.{TrackerImpl, TrackerMap}
import com.daml.platform.server.api.ApiException
import com.daml.platform.server.api.services.grpc.GrpcCommandService
import com.daml.util.Ctx
import com.daml.util.akkastreams.MaxInFlight
import com.google.protobuf.empty.Empty
import io.grpc._
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

final class ApiCommandService private (
    services: LocalServices,
    configuration: ApiCommandService.Configuration,
)(
    implicit grpcExecutionContext: ExecutionContext,
    actorMaterializer: Materializer,
    esf: ExecutionSequencerFactory,
    logCtx: LoggingContext
) extends CommandServiceGrpc.CommandService
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val submissionTracker: TrackerMap = TrackerMap(configuration.retentionPeriod)
  private val staleCheckerInterval: FiniteDuration = 30.seconds

  private val trackerCleanupJob: Cancellable = actorMaterializer.system.scheduler
    .scheduleAtFixedRate(staleCheckerInterval, staleCheckerInterval)(submissionTracker.cleanup)

  @volatile private var running = true

  override def close(): Unit = {
    logger.info("Shutting down Command Service")
    trackerCleanupJob.cancel()
    running = false
    submissionTracker.close()
  }

  private def submitAndWaitInternal(request: SubmitAndWaitRequest): Future[Completion] =
    withEnrichedLoggingContext(
      logging.commandId(request.getCommands.commandId),
      logging.party(request.getCommands.party)) { implicit logCtx =>
      if (running) {
        track(request)
      } else {
        Future.failed(
          new ApiException(Status.UNAVAILABLE.withDescription("Service has been shut down.")))
      }.andThen(logger.logErrorsOnCall[Completion])
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def track(request: SubmitAndWaitRequest): Future[Completion] = {
    val appId = request.getCommands.applicationId
    val submitter = TrackerMap.Key(application = appId, party = request.getCommands.party)
    submissionTracker.track(submitter, request) {
      for {
        ledgerEnd <- services.getCompletionEnd().map(_.getOffset)
      } yield {
        val tracker = CommandTrackerFlow[Promise[Completion], NotUsed](
          services.submissionFlow,
          offset =>
            services
              .getCompletionSource(
                CompletionStreamRequest(
                  configuration.ledgerId.unwrap,
                  appId,
                  List(submitter.party),
                  Some(offset)))
              .mapConcat(CommandCompletionSource.toStreamElements),
          ledgerEnd,
          () => configuration.maxDeduplicationTime
        )
        val trackingFlow =
          if (configuration.limitMaxCommandsInFlight)
            MaxInFlight(configuration.maxCommandsInFlight).joinMat(tracker)(Keep.right)
          else
            tracker
        TrackerImpl(trackingFlow, configuration.inputBufferSize)
      }
    }
  }

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    submitAndWaitInternal(request).map(_ => Empty.defaultInstance)

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionIdResponse] =
    submitAndWaitInternal(request).map { compl =>
      SubmitAndWaitForTransactionIdResponse(compl.transactionId)
    }

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionResponse] =
    submitAndWaitInternal(request).flatMap { resp =>
      val txRequest = GetTransactionByIdRequest(
        request.getCommands.ledgerId,
        resp.transactionId,
        List(request.getCommands.party))
      services
        .getFlatTransactionById(txRequest)
        .map(resp => SubmitAndWaitForTransactionResponse(resp.transaction))
    }

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionTreeResponse] =
    submitAndWaitInternal(request).flatMap { resp =>
      val txRequest = GetTransactionByIdRequest(
        request.getCommands.ledgerId,
        resp.transactionId,
        List(request.getCommands.party))
      services
        .getTransactionById(txRequest)
        .map(resp => SubmitAndWaitForTransactionTreeResponse(resp.transaction))
    }

  override def toString: String = ApiCommandService.getClass.getSimpleName
}

object ApiCommandService {

  def create(
      configuration: Configuration,
      services: LocalServices,
      timeProvider: TimeProvider,
  )(
      implicit grpcExecutionContext: ExecutionContext,
      actorMaterializer: Materializer,
      esf: ExecutionSequencerFactory,
      logCtx: LoggingContext
  ): CommandServiceGrpc.CommandService with GrpcApiService =
    new GrpcCommandService(
      new ApiCommandService(services, configuration),
      ledgerId = configuration.ledgerId,
      currentLedgerTime = () => timeProvider.getCurrentTime,
      currentUtcTime = () => Instant.now,
      maxDeduplicationTime = () => configuration.maxDeduplicationTime,
    )

  final case class Configuration(
      ledgerId: LedgerId,
      inputBufferSize: Int,
      maxParallelSubmissions: Int,
      maxCommandsInFlight: Int,
      limitMaxCommandsInFlight: Boolean,
      retentionPeriod: FiniteDuration,
      // TODO(RA): this should be updated dynamically from the ledger configuration
      maxDeduplicationTime: java.time.Duration,
  )

  final case class LocalServices(
      submissionFlow: Flow[
        Ctx[(Promise[Completion], String), SubmitRequest],
        Ctx[(Promise[Completion], String), Try[Empty]],
        NotUsed],
      getCompletionSource: CompletionStreamRequest => Source[CompletionStreamResponse, NotUsed],
      getCompletionEnd: () => Future[CompletionEndResponse],
      getTransactionById: GetTransactionByIdRequest => Future[GetTransactionResponse],
      getFlatTransactionById: GetTransactionByIdRequest => Future[GetFlatTransactionResponse],
  )

}
