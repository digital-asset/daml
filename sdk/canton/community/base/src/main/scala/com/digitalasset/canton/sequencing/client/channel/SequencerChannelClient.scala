// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason
import com.digitalasset.canton.sequencing.protocol.SequencerChannelId
import com.digitalasset.canton.topology.{Member, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Context

import scala.concurrent.ExecutionContext
import scala.util.Try

/** Sequencer channel client for interacting with GRPC-based sequencer channels that exchange data with other nodes
  * bypassing the regular canton protocol and ordering-based sequencer service. In addition, the
  * [[SequencerChannelClient]] tracks channels allowing the client to close channels when the client is closed.
  *
  * For now at least the sequencer channel client uses the canton-protocol version although technically the channel
  * protocol is a different protocol.
  */
final class SequencerChannelClient(
    member: Member,
    clientState: SequencerChannelClientState,
    domainParameters: StaticDomainParameters,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {

  /** Connect to a sequencer channel. If the channel does not yet exist on the service side,
    * this call creates the sequencer channel.
    *
    * This method creates the channel asynchronously and relies on the provided [[SequencerChannelProtocolProcessor]]
    * to perform the bidirectional interaction with the channel.
    *
    * @param sequencerId [[com.digitalasset.canton.topology.SequencerId]] of the sequencer to create the channel on
    * @param channelId   Unique channel identifier known to both channel endpoints
    * @param connectTo   Member id of the other member to communicate with via the channel
    * @param processor   Sequencer channel protocol processor for handling incoming messages and sending messages
    */
  def connectToSequencerChannel(
      sequencerId: SequencerId,
      channelId: SequencerChannelId,
      connectTo: Member,
      processor: SequencerChannelProtocolProcessor,
  )(implicit traceContext: TraceContext): EitherT[UnlessShutdown, String, Unit] = {

    // Callback to remove channel tracking state and notify the processor of the channel close
    def closeChannelEndpoint(
        closeReasonTry: Try[SubscriptionCloseReason[String]]
    )(implicit traceContext: TraceContext): Unit = {
      clientState.closeChannelEndpoint(sequencerId, channelId).leftMap(logger.warn(_)).merge
      processor.handleClose(s"Sequencer channel endpoint $channelId", closeReasonTry)
    }

    EitherT(performUnlessClosing("connectToSequencerChannel") {
      for {
        transport <- clientState.transport(sequencerId)
        endpoint = new SequencerChannelClientEndpoint(
          sequencerId,
          channelId,
          member,
          connectTo,
          processor,
          domainParameters,
          Context.ROOT.withCancellation(),
          timeouts,
          loggerFactory
            .append("sequencerId", sequencerId.uid.toString)
            .append("channel", channelId.unwrap),
        )
        _ <- processor.setChannelEndpoint(endpoint)
        _ <- clientState.addChannelEndpoint(endpoint)
      } yield {
        endpoint.closeReason.onComplete(closeChannelEndpoint)
        transport.connectToSequencerChannel(endpoint)
      }
    })
  }

  /** Ping the sequencer to check if the sequencer with the provided SequencerId supports sequencer channels.
    */
  def ping(sequencerId: SequencerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = for {
    transport <- EitherT.fromEither[FutureUnlessShutdown](clientState.transport(sequencerId))
    _ <- transport.ping()
  } yield ()

  override protected def onClosed(): Unit = clientState.close()
}
