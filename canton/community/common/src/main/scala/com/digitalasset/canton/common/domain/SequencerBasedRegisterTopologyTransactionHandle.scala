// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.domain

import cats.data.EitherT
import cats.syntax.functorFilter.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.{ProcessingTimeout, TopologyXConfig}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.mapErr
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{
  ApplicationHandler,
  EnvelopeHandler,
  HandlerResult,
  NoEnvelopeBox,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyChangeOp}
import com.digitalasset.canton.topology.{DomainId, DomainTopologyManagerId, Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, FutureUtil}
import com.digitalasset.canton.version.ProtocolVersion

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
    send: (
        TraceContext,
        OpenEnvelope[ProtocolMessage],
    ) => EitherT[Future, SendAsyncClientError, Unit],
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
    new DomainTopologyService(domainId, send, protocolVersion, timeouts, loggerFactory)

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

class SequencerBasedRegisterTopologyTransactionHandleX(
    sequencerClient: SequencerClient,
    val domainId: DomainId,
    val member: Member,
    clock: Clock,
    topologyXConfig: TopologyXConfig,
    protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends RegisterTopologyTransactionHandleWithProcessor[
      GenericSignedTopologyTransactionX,
      TopologyTransactionsBroadcastX.State,
    ]
    with NamedLogging
    with PrettyPrinting {

  private val service =
    new DomainTopologyServiceX(
      sequencerClient,
      clock,
      topologyXConfig,
      protocolVersion,
      timeouts,
      loggerFactory,
    )

  // we don't need to register a specific message handler, because we use SequencerClientSend's SendTracker
  override val processor: EnvelopeHandler =
    ApplicationHandler.success[NoEnvelopeBox, DefaultOpenEnvelope]()

  override def submit(
      transactions: Seq[GenericSignedTopologyTransactionX]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyTransactionsBroadcastX.State]] = {
    service.registerTopologyTransaction(
      TopologyTransactionsBroadcastX.create(
        domainId,
        List(
          TopologyTransactionsBroadcastX
            .Broadcast(String255.tryCreate(UUID.randomUUID().toString), transactions.toList)
        ),
        protocolVersion,
      )
    )
  }

  override def onClosed(): Unit = service.close()

  override def pretty: Pretty[SequencerBasedRegisterTopologyTransactionHandleX.this.type] =
    prettyOfClass(
      param("domainId", _.domainId),
      param("member", _.member),
    )
}

class DomainTopologyService(
    domainId: DomainId,
    send: (
        TraceContext,
        OpenEnvelope[ProtocolMessage],
    ) => EitherT[Future, SendAsyncClientError, Unit],
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
    for {
      _ <- performUnlessClosingF(functionFullName)(
        EitherTUtil.toFuture(mapErr(sendRequest(request)))
      )
      response <- responseF
    } yield response
  }

  private def sendRequest(
      request: Request
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] = {
    logger.debug(s"Sending register topology transaction request ${request}")
    EitherTUtil.logOnError(
      send(
        traceContext,
        OpenEnvelope(request, recipients)(protocolVersion),
      ),
      s"Failed sending register topology transaction request ${request}",
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

class DomainTopologyServiceX(
    sequencerClient: SequencerClient,
    clock: Clock,
    topologyXConfig: TopologyXConfig,
    protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  def registerTopologyTransaction(
      request: TopologyTransactionsBroadcastX
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyTransactionsBroadcastX.State]] = {
    val sendCallback = SendCallback.future

    performUnlessClosingEitherUSF(
      functionFullName
    )(
      sendRequest(request, sendCallback)
        .mapK(FutureUnlessShutdown.outcomeK)
        .biSemiflatMap(
          sendAsyncClientError => {
            logger.error(s"Failed broadcasting topology transactions: $sendAsyncClientError")
            FutureUnlessShutdown.pure[TopologyTransactionsBroadcastX.State](
              TopologyTransactionsBroadcastX.State.Failed
            )
          },
          _result =>
            sendCallback.future
              .map {
                case SendResult.Success(_) =>
                  TopologyTransactionsBroadcastX.State.Accepted
                case notSequenced @ (_: SendResult.Timeout | _: SendResult.Error) =>
                  logger.info(
                    s"The submitted topology transactions were not sequenced. Error=[$notSequenced]. Transactions=${request.broadcasts}"
                  )
                  TopologyTransactionsBroadcastX.State.Failed
              },
        )
    ).merge
      .map(Seq.fill(request.broadcasts.flatMap(_.transactions).size)(_))
  }

  private def sendRequest(
      request: TopologyTransactionsBroadcastX,
      sendCallback: SendCallback,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] = {
    logger.debug(s"Broadcasting topology transaction: ${request.broadcasts}")
    EitherTUtil.logOnError(
      sequencerClient.sendAsyncUnauthenticatedOrNot(
        Batch.of(protocolVersion, (request, Recipients.cc(TopologyBroadcastAddress.recipient))),
        maxSequencingTime =
          clock.now.add(topologyXConfig.topologyTransactionRegistrationTimeout.toInternal.duration),
        callback = sendCallback,
      ),
      s"Failed sending topology transaction broadcast: ${request}",
    )
  }
}
