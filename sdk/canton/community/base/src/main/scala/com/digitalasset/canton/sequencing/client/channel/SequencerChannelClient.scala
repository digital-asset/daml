// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason
import com.digitalasset.canton.sequencing.client.channel.endpoint.SequencerChannelClientEndpoint
import com.digitalasset.canton.sequencing.client.channel.endpoint.SequencerChannelClientEndpoint.OnSentMessageForTesting
import com.digitalasset.canton.sequencing.protocol.channel.SequencerChannelId
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
    domainCryptoApi: DomainSyncCryptoClient,
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
    * One of the connecting members is responsible for generating a session key to secure the communication between
    * them, that member "owns" the session key. The decision which member owns the session key is taken by the client
    * of the sequencer channel.
    *
    * The session key is transferred to another member through public key encryption. The public key is determined by
    * a timestamp that been agreed upon by the connecting members beforehand.
    * This timestamp is expected to originate from a recent topology transaction that has already taken effect.
    *
    * @param sequencerId [[com.digitalasset.canton.topology.SequencerId]] of the sequencer to create the channel on
    * @param channelId   Unique channel identifier known to both channel endpoints
    * @param connectTo   Member id of the other member to communicate with via the channel
    * @param processor   Sequencer channel protocol processor for handling incoming messages and sending messages
    * @param isSessionKeyOwner Whether this member owns the session key
    * @param topologyTs Timestamp that determines the public key encryption of the session key
    * @param onSentMessage Message notification for testing only! None for production.
    */
  def connectToSequencerChannel(
      sequencerId: SequencerId,
      channelId: SequencerChannelId,
      connectTo: Member,
      processor: SequencerChannelProtocolProcessor,
      isSessionKeyOwner: Boolean,
      topologyTs: CantonTimestamp,
      onSentMessage: Option[OnSentMessageForTesting] = None,
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
          domainCryptoApi,
          isSessionKeyOwner,
          topologyTs,
          domainParameters.protocolVersion,
          Context.ROOT.withCancellation(),
          timeouts,
          loggerFactory
            .append("sequencerId", sequencerId.uid.toString)
            .append("channel", channelId.unwrap),
          onSentMessage,
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
