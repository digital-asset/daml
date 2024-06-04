// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.domain

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.{ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersion

import java.util.UUID
import scala.concurrent.ExecutionContext

// unsealed for testing
trait RegisterTopologyTransactionHandle extends FlagCloseable {
  def submit(transactions: Seq[GenericSignedTopologyTransaction])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyTransactionsBroadcast.State]]
}

class SequencerBasedRegisterTopologyTransactionHandle(
    sequencerClient: SequencerClient,
    val domainId: DomainId,
    val member: Member,
    clock: Clock,
    topologyConfig: TopologyConfig,
    protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends RegisterTopologyTransactionHandle
    with NamedLogging
    with PrettyPrinting {

  private val service =
    new DomainTopologyService(
      sequencerClient,
      clock,
      topologyConfig,
      protocolVersion,
      timeouts,
      loggerFactory,
    )

  override def submit(
      transactions: Seq[GenericSignedTopologyTransaction]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyTransactionsBroadcast.State]] = {
    service.registerTopologyTransaction(
      TopologyTransactionsBroadcast.create(
        domainId,
        List(
          TopologyTransactionsBroadcast
            .Broadcast(String255.tryCreate(UUID.randomUUID().toString), transactions.toList)
        ),
        protocolVersion,
      )
    )
  }

  override def onClosed(): Unit = service.close()

  override def pretty: Pretty[SequencerBasedRegisterTopologyTransactionHandle.this.type] =
    prettyOfClass(
      param("domainId", _.domainId),
      param("member", _.member),
    )
}

class DomainTopologyService(
    sequencerClient: SequencerClient,
    clock: Clock,
    topologyConfig: TopologyConfig,
    protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  def registerTopologyTransaction(
      request: TopologyTransactionsBroadcast
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyTransactionsBroadcast.State]] = {
    val sendCallback = SendCallback.future

    performUnlessClosingEitherUSF(
      functionFullName
    )(
      sendRequest(request, sendCallback)
        .biSemiflatMap(
          sendAsyncClientError => {
            logger.error(s"Failed broadcasting topology transactions: $sendAsyncClientError")
            FutureUnlessShutdown.pure[TopologyTransactionsBroadcast.State](
              TopologyTransactionsBroadcast.State.Failed
            )
          },
          _result =>
            sendCallback.future
              .map {
                case SendResult.Success(_) =>
                  TopologyTransactionsBroadcast.State.Accepted
                case notSequenced @ (_: SendResult.Timeout | _: SendResult.Error) =>
                  logger.info(
                    s"The submitted topology transactions were not sequenced. Error=[$notSequenced]. Transactions=${request.broadcasts}"
                  )
                  TopologyTransactionsBroadcast.State.Failed
              },
        )
    ).merge
      .map(Seq.fill(request.broadcasts.flatMap(_.transactions).size)(_))
  }

  private def sendRequest(
      request: TopologyTransactionsBroadcast,
      sendCallback: SendCallback,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientError, Unit] = {
    logger.debug(s"Broadcasting topology transaction: ${request.broadcasts}")
    EitherTUtil.logOnErrorU(
      sequencerClient.sendAsync(
        Batch.of(protocolVersion, (request, Recipients.cc(TopologyBroadcastAddress.recipient))),
        maxSequencingTime =
          clock.now.add(topologyConfig.topologyTransactionRegistrationTimeout.toInternal.duration),
        callback = sendCallback,
        // Do not amplify because we are running our own retry loop here anyway
        amplify = false,
      ),
      s"Failed sending topology transaction broadcast: ${request}",
    )
  }
}
