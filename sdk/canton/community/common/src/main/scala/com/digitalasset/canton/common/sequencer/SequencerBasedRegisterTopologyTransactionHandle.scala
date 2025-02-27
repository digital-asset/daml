// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.sequencer

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.{ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersion
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext

// unsealed for testing
trait RegisterTopologyTransactionHandle extends FlagCloseable {
  def submit(transactions: Seq[GenericSignedTopologyTransaction])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyTransactionsBroadcast.State]]
}

class SequencerBasedRegisterTopologyTransactionHandle(
    sequencerClient: SequencerClient,
    val synchronizerId: SynchronizerId,
    val member: Member,
    timeTracker: SynchronizerTimeTracker,
    clock: Clock,
    topologyConfig: TopologyConfig,
    protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends RegisterTopologyTransactionHandle
    with NamedLogging
    with PrettyPrinting {

  override def submit(
      transactions: Seq[GenericSignedTopologyTransaction]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyTransactionsBroadcast.State]] = {
    val sendCallback = SendCallback.future
    val maxSequencingTime =
      clock.now.add(topologyConfig.topologyTransactionRegistrationTimeout.toInternal.duration)
    val request = TopologyTransactionsBroadcast(
      synchronizerId,
      transactions,
      protocolVersion,
    )
    performUnlessClosingEitherUSF(functionFullName)(
      sendRequest(request, maxSequencingTime, sendCallback)
    )
      .biSemiflatMap(
        sendAsyncClientError => {
          logger.warn(
            s"Failed broadcasting topology transactions: $sendAsyncClientError. This will be retried automatically."
          )
          FutureUnlessShutdown.pure[TopologyTransactionsBroadcast.State](
            TopologyTransactionsBroadcast.State.Failed
          )
        },
        _ => {
          // request a tick for maxSequencing time, so that a node with no traffic
          // can still determine whether the topology broadcast timed out or not.
          timeTracker.requestTick(maxSequencingTime)
          sendCallback.future
            .map {
              case SendResult.Success(_) =>
                TopologyTransactionsBroadcast.State.Accepted
              case notSequenced @ (_: SendResult.Timeout | _: SendResult.Error) =>
                logger.info(
                  s"The submitted topology transactions were not sequenced. Error=[$notSequenced]. Transactions=${request.transactions}"
                )
                TopologyTransactionsBroadcast.State.Failed
            }
        },
      )
      .merge
      .map(Seq.fill(request.signedTransactions.size)(_))

  }

  private def sendRequest(
      request: TopologyTransactionsBroadcast,
      maxSequencingTime: CantonTimestamp,
      sendCallback: SendCallback,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientError, Unit] = {
    implicit val metricsContext: MetricsContext = MetricsContext(
      "type" -> "send-topology"
    )
    logger.debug(s"Broadcasting topology transaction: ${request.transactions}")
    EitherTUtil.logOnErrorU(
      sequencerClient.sendAsync(
        Batch.of(protocolVersion, (request, Recipients.cc(TopologyBroadcastAddress.recipient))),
        maxSequencingTime = maxSequencingTime,
        callback = sendCallback,
        // Do not amplify because we are running our own retry loop here anyway
        amplify = false,
      ),
      s"Failed sending topology transaction broadcast: $request. This will be retried automatically.",
      level = Level.WARN,
    )
  }

  override protected def pretty: Pretty[SequencerBasedRegisterTopologyTransactionHandle.this.type] =
    prettyOfClass(
      param("synchronizerId", _.synchronizerId),
      param("member", _.member),
    )
}
