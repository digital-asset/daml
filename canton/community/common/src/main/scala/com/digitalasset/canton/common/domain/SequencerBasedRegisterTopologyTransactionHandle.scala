// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.domain

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.{ProcessingTimeout, TopologyXConfig}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{ApplicationHandler, EnvelopeHandler, NoEnvelopeBox}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersion

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

// unsealed for testing
trait RegisterTopologyTransactionHandle extends FlagCloseable {
  def submit(transactions: Seq[GenericSignedTopologyTransactionX])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyTransactionsBroadcastX.State]]

  // we don't need to register a specific message handler, because we use SequencerClientSend's SendTracker
  val processor: EnvelopeHandler =
    ApplicationHandler.success[NoEnvelopeBox, DefaultOpenEnvelope]()
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
    extends RegisterTopologyTransactionHandle
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
