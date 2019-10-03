// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.command

import akka.NotUsed
import akka.actor.Cancellable
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Source}
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.command_completion_service.{
  CompletionEndResponse,
  CompletionStreamRequest,
  CompletionStreamResponse
}
import com.digitalasset.ledger.api.v1.command_service._
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionByIdRequest,
  GetTransactionResponse
}
import com.digitalasset.ledger.client.services.commands.{
  CommandCompletionSource,
  CommandTrackerFlow
}
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ApiException
import com.digitalasset.platform.server.api.services.grpc.GrpcCommandService
import com.digitalasset.platform.server.services.command.ApiCommandService.LowLevelCommandServiceAccess
import com.digitalasset.util.Ctx
import com.digitalasset.util.akkastreams.MaxInFlight
import com.google.protobuf.empty.Empty

import io.grpc._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import scalaz.syntax.tag._

class ApiCommandService private (
    lowLevelCommandServiceAccess: LowLevelCommandServiceAccess,
    configuration: ApiCommandService.Configuration,
    loggerFactory: NamedLoggerFactory)(
    implicit grpcExecutionContext: ExecutionContext,
    actorMaterializer: ActorMaterializer,
    esf: ExecutionSequencerFactory)
    extends CommandServiceGrpc.CommandService
    with AutoCloseable {

  private val logger = loggerFactory.getLogger(this.getClass)

  private type CommandId = String
  private type ApplicationId = String

  private val submissionTracker: TrackerMap =
    TrackerMap(configuration.retentionPeriod, loggerFactory)
  private val staleCheckerInterval: FiniteDuration = 30.seconds

  private val trackerCleanupJob: Cancellable = actorMaterializer.system.scheduler.schedule(
    staleCheckerInterval,
    staleCheckerInterval,
    submissionTracker.cleanup
  )

  @volatile private var running = true

  override def close(): Unit = {
    logger.info("Shutting down Command Service")
    trackerCleanupJob.cancel()
    running = false
    submissionTracker.close()
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def submitAndWaitInternal(request: SubmitAndWaitRequest): Future[Completion] = {

    val appId = request.getCommands.applicationId
    val submitter = TrackerMap.Key(application = appId, party = request.getCommands.party)

    if (running) {
      submissionTracker
        .track(submitter, request) {
          for {
            trackingFlow <- {
              lowLevelCommandServiceAccess match {
                case LowLevelCommandServiceAccess.LocalServices(
                    submissionFlow,
                    getCompletionSource,
                    getCompletionEnd,
                    _,
                    _) =>
                  for {
                    ledgerEnd <- getCompletionEnd().map(_.getOffset)
                  } yield {
                    val tracker =
                      CommandTrackerFlow[Promise[Completion], NotUsed](
                        submissionFlow,
                        offset =>
                          getCompletionSource(
                            CompletionStreamRequest(
                              configuration.ledgerId.unwrap,
                              appId,
                              List(submitter.party),
                              Some(offset)))
                            .mapConcat(CommandCompletionSource.toStreamElements),
                        ledgerEnd
                      )

                    if (configuration.limitMaxCommandsInFlight)
                      MaxInFlight(configuration.maxCommandsInFlight).joinMat(tracker)(Keep.right)
                    else tracker
                  }
              }
            }
          } yield {
            TrackerImpl(trackingFlow, configuration.inputBufferSize, configuration.historySize)
          }
        }
    } else {
      Future.failed(
        new ApiException(Status.UNAVAILABLE.withDescription("Service has been shut down.")))
    }
  }

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] = {
    submitAndWaitInternal(request).map(_ => Empty.defaultInstance)
  }

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionIdResponse] = {
    submitAndWaitInternal(request).map { compl =>
      SubmitAndWaitForTransactionIdResponse(compl.transactionId)
    }
  }

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionResponse] = {
    submitAndWaitInternal(request).flatMap { resp =>
      val txRequest = GetTransactionByIdRequest(
        request.getCommands.ledgerId,
        resp.transactionId,
        List(request.getCommands.party))
      flatById(txRequest).map(resp => SubmitAndWaitForTransactionResponse(resp.transaction))

    }
  }

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest): Future[SubmitAndWaitForTransactionTreeResponse] = {
    submitAndWaitInternal(request).flatMap { resp =>
      val txRequest = GetTransactionByIdRequest(
        request.getCommands.ledgerId,
        resp.transactionId,
        List(request.getCommands.party))

      treeById(txRequest).map(resp => SubmitAndWaitForTransactionTreeResponse(resp.transaction))
    }
  }

  override def toString: String = ApiCommandService.getClass.getSimpleName

  private val (treeById, flatById) = {
    lowLevelCommandServiceAccess match {
      case LowLevelCommandServiceAccess.LocalServices(
          _,
          _,
          _,
          getTransactionById,
          getFlatTransactionById) =>
        (getTransactionById, getFlatTransactionById)
    }
  }
}

object ApiCommandService {

  def create(
      configuration: Configuration,
      svcAccess: LowLevelCommandServiceAccess,
      loggerFactory: NamedLoggerFactory)(
      implicit grpcExecutionContext: ExecutionContext,
      actorMaterializer: ActorMaterializer,
      esf: ExecutionSequencerFactory
  ): CommandServiceGrpc.CommandService with GrpcApiService with CommandServiceLogging =
    new GrpcCommandService(
      new ApiCommandService(svcAccess, configuration, loggerFactory),
      configuration.ledgerId
    ) with CommandServiceLogging

  final case class Configuration(
      ledgerId: LedgerId,
      inputBufferSize: Int,
      maxParallelSubmissions: Int,
      maxCommandsInFlight: Int,
      limitMaxCommandsInFlight: Boolean,
      historySize: Int,
      retentionPeriod: FiniteDuration,
      commandTtl: FiniteDuration)

  sealed abstract class LowLevelCommandServiceAccess extends Product with Serializable

  object LowLevelCommandServiceAccess {

    final case class LocalServices(
        submissionFlow: Flow[
          Ctx[(Promise[Completion], String), SubmitRequest],
          Ctx[(Promise[Completion], String), Try[Empty]],
          NotUsed],
        getCompletionSource: CompletionStreamRequest => Source[CompletionStreamResponse, NotUsed],
        getCompletionEnd: () => Future[CompletionEndResponse],
        getTransactionById: GetTransactionByIdRequest => Future[GetTransactionResponse],
        getFlatTransactionById: GetTransactionByIdRequest => Future[GetFlatTransactionResponse])
        extends LowLevelCommandServiceAccess

  }

}
