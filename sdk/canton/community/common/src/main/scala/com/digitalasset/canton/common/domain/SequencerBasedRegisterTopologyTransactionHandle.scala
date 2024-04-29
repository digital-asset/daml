// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.domain

import cats.data.EitherT
import cats.syntax.functorFilter.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.SendCallback.CallbackFuture
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{
  ApplicationHandler,
  EnvelopeHandler,
  HandlerResult,
  NoEnvelopeBox,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyChangeOp}
import com.digitalasset.canton.topology.{DomainId, DomainTopologyManagerId, Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, FutureUtil}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Status

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*

trait RegisterTopologyTransactionHandleCommon[TX, State] extends FlagCloseable {
  def submit(transactions: Seq[TX])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[State]]
}

trait RegisterTopologyTransactionHandleWithProcessor[TX, State]
    extends RegisterTopologyTransactionHandleCommon[TX, State] {
  def processor: EnvelopeHandler
}

/** Handle used in order to request approval of participant's topology transactions by the IDM and wait for the
  * responses by sending RegisterTopologyTransactionRequest's via the sequencer.
  * This gets created in [[com.digitalasset.canton.participant.topology.ParticipantTopologyDispatcher]]
  */
class SequencerBasedRegisterTopologyTransactionHandle(
    sender: SequencerBasedRegisterTopologyTransactionHandle.Sender,
    domainId: DomainId,
    participantId: ParticipantId,
    requestedBy: Member,
    protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends RegisterTopologyTransactionHandleWithProcessor[
      SignedTopologyTransaction[TopologyChangeOp],
      RegisterTopologyTransactionResponseResult.State,
    ]
    with NamedLogging {

  private val service =
    new DomainTopologyService(domainId, sender, protocolVersion, timeouts, loggerFactory)

  // must be used by the event handler of a sequencer subscription in order to complete the promises of requests sent with the given sequencer client
  override val processor: EnvelopeHandler = service.processor

  override def submit(
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[RegisterTopologyTransactionResponseResult.State]] =
    service.registerTopologyTransaction(
      RegisterTopologyTransactionRequest
        .create(
          requestedBy = requestedBy,
          participant = participantId,
          requestId = String255.tryCreate(UUID.randomUUID().toString),
          transactions = transactions.toList,
          domainId = domainId,
          protocolVersion = protocolVersion,
        )
    )

  override def onClosed(): Unit = service.close()
}

object SequencerBasedRegisterTopologyTransactionHandle {

  trait Sender {

    def send(
        envelope: OpenEnvelope[ProtocolMessage],
        timeout: NonNegativeFiniteDuration,
    )(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncClientError, FutureUnlessShutdown[SendResult]]

  }

  abstract class SenderImpl(clock: Clock, protocolVersion: ProtocolVersion)(implicit
      executionContext: ExecutionContext
  ) extends Sender {

    protected def sendInternal(
        batch: Batch[DefaultOpenEnvelope],
        maxSequencingTime: CantonTimestamp,
        callback: SendCallback,
    )(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncClientError, Unit]

    override def send(
        envelope: OpenEnvelope[ProtocolMessage],
        timeout: NonNegativeFiniteDuration,
    )(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncClientError, FutureUnlessShutdown[SendResult]] = {
      val fut = new CallbackFuture()
      sendInternal(
        Batch(List(envelope), protocolVersion),
        maxSequencingTime = clock.now.add(timeout.duration),
        callback = fut,
        // return callback, as we might have to wait a bit for the domain to respond, hence risking a shutdown issue
      ).map { _ => fut.future }
    }
  }

}

class DomainTopologyService(
    domainId: DomainId,
    sender: SequencerBasedRegisterTopologyTransactionHandle.Sender,
    protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  type RequestIndex = (TopologyRequestId, ParticipantId)
  type Request = RegisterTopologyTransactionRequest
  type Response = RegisterTopologyTransactionResponse.Result
  type Result = Seq[RegisterTopologyTransactionResponseResult.State]

  val recipients = Recipients.cc(DomainTopologyManagerId(domainId))
  protected def requestToIndex(
      request: RegisterTopologyTransactionRequest
  ): (TopologyRequestId, ParticipantId) = (request.requestId, request.participant)

  protected def responseToIndex(
      response: RegisterTopologyTransactionResponse.Result
  ): (TopologyRequestId, ParticipantId) = (response.requestId, response.participant)

  protected def responseToResult(
      response: RegisterTopologyTransactionResponse.Result
  ): Seq[RegisterTopologyTransactionResponseResult.State] = response.results.map(_.state)

  protected def protocolMessageToResponse(
      m: ProtocolMessage
  ): Option[RegisterTopologyTransactionResponse.Result] = m match {
    case m: RegisterTopologyTransactionResponse.Result => Some(m)
    case _ => None
  }

  private val responsePromiseMap: concurrent.Map[RequestIndex, Promise[UnlessShutdown[Result]]] =
    new ConcurrentHashMap[RequestIndex, Promise[UnlessShutdown[Result]]]().asScala

  def registerTopologyTransaction(
      request: Request
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Result] = {

    val responseF = getResponse(request)
    val ret = for {
      sendResultFU <- performUnlessClosingEitherU(functionFullName)(
        sendRequest(request)
      )
      _ <- EitherT(
        sendResultFU
          .map {
            case SendResult.Success(_) =>
              Right(())
            case failed: SendResult.NotSequenced =>
              Left(SendAsyncClientError.RequestFailed(failed.toString): SendAsyncClientError)
          }
      )
      response <- EitherT.right[SendAsyncClientError](responseF)
    } yield response
    val tmp = ret.leftMap { err =>
      // clean up on left
      responsePromiseMap.remove(requestToIndex(request)).discard
      Status.ABORTED.withDescription(err.toString).asRuntimeException()
    }
    EitherTUtil.toFutureUnlessShutdown(tmp)
  }

  private def sendRequest(
      request: Request
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, FutureUnlessShutdown[SendResult]] = {
    logger.debug(s"Sending register topology transaction request ${request}")
    sender.send(
      OpenEnvelope(request, recipients)(protocolVersion),
      timeouts.topologyDispatching.toInternal,
    )
  }

  private def getResponse(request: Request)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Result] =
    FutureUnlessShutdown {
      val promise = Promise[UnlessShutdown[Result]]()
      responsePromiseMap.put(requestToIndex(request), promise).discard
      FutureUtil.logOnFailure(
        promise.future.map { result =>
          result match {
            case _ @UnlessShutdown.Outcome(_) =>
              logger.debug(
                s"Received register topology transaction response ${request}"
              )
            case _: UnlessShutdown.AbortedDueToShutdown =>
              logger.info(
                show"Shutdown before receiving register topology transaction response ${request}"
              )
          }
          result
        },
        show"Failed to receive register topology transaction response ${request}",
      )
    }

  val processor: EnvelopeHandler =
    ApplicationHandler.create[NoEnvelopeBox, DefaultOpenEnvelope](
      "handle-topology-request-responses"
    )(envs =>
      envs.withTraceContext { implicit traceContext => envs =>
        HandlerResult.asynchronous(performUnlessClosingF(s"${getClass.getSimpleName}-processor") {
          Future {
            envs.mapFilter(env => protocolMessageToResponse(env.protocolMessage)).foreach {
              response =>
                responsePromiseMap
                  .remove(responseToIndex(response))
                  .foreach(_.trySuccess(UnlessShutdown.Outcome(responseToResult(response))))
            }
          }
        })
      }
    )

  override def onClosed(): Unit = {
    responsePromiseMap.values.foreach(
      _.trySuccess(UnlessShutdown.AbortedDueToShutdown).discard[Boolean]
    )
  }
}
