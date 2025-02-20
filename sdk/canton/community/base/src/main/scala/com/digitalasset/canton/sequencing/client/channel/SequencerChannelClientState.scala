// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, OnShutdownRunner}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.channel.endpoint.SequencerChannelClientEndpoint
import com.digitalasset.canton.sequencing.protocol.channel.SequencerChannelId
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Context.CancellableContext

import scala.collection.mutable
import scala.concurrent.{Future, Promise, blocking}
import scala.util.Success

/** SequencerChannelClientState holds the set of (static) channel transports and dynamically
  * changing per-sequencer channel state.
  */
private[channel] final class SequencerChannelClientState(
    transportsMap: NonEmpty[Map[SequencerId, SequencerChannelClientTransport]],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with FlagCloseable {
  private val transports: NonEmpty[Map[SequencerId, SequencerChannelState]] =
    transportsMap.map { case (sequencerId, transport) =>
      sequencerId -> new SequencerChannelState(transport, loggerFactory)
    }.toMap

  def transport(sequencerId: SequencerId): Either[String, SequencerChannelClientTransport] =
    transports
      .get(sequencerId)
      .map(_.transport)
      .toRight(s"Sequencer id $sequencerId not found")

  // Promise completed upon sequencer channel client shutdown or
  // TODO(#21120) when an unexpected channel error occurs.
  private val closePromise = Promise[Unit]()

  def completion: Future[Unit] = closePromise.future

  def connectToSequencer(
      sequencerId: SequencerId,
      processor: SequencerChannelProtocolProcessor,
      endpointFactory: (CancellableContext, OnShutdownRunner) => SequencerChannelClientEndpoint,
  )(implicit traceContext: TraceContext): Either[String, SequencerChannelClientEndpoint] = {
    def endpointFactoryWithRegistration(state: SequencerChannelState)(
        context: CancellableContext,
        onShutdownRunner: OnShutdownRunner,
    ): Either[String, SequencerChannelClientEndpoint] = {
      val endpoint = endpointFactory(context, onShutdownRunner)
      for {
        _ <- state.addChannelEndpoint(endpoint)
        _ <- processor.setChannelEndpoint(endpoint)
      } yield endpoint
    }

    for {
      state <- transports.get(sequencerId).toRight(s"Sequencer id $sequencerId not found")
      endpoint <- state.transport.connectToSequencerChannel(
        endpointFactoryWithRegistration(state)
      )
    } yield endpoint
  }

  def closeChannelEndpoint(
      sequencerId: SequencerId,
      channelId: SequencerChannelId,
  ): Either[String, Unit] =
    modifyChannelState(sequencerId, _.closeChannelEndpoint(channelId))

  private def modifyChannelState(
      sequencerId: SequencerId,
      modify: SequencerChannelState => Either[String, Unit],
  ): Either[String, Unit] = for {
    sequencerChannelState <- transports
      .get(sequencerId)
      .toRight(s"Sequencer id $sequencerId not found")
    _ <- modify(sequencerChannelState)
  } yield ()

  private def closeChannelsAndTransports(): Unit = {
    transports.foreach { case (_, channelState) =>
      channelState.closeChannelsAndTransport()
    }

    closePromise.tryComplete(Success(())).discard
  }

  override protected def onClosed(): Unit =
    closeChannelsAndTransports()
}

/** SequencerChannelState holds the sequencer-specific channel transport and manages the set of
  * dynamically changing channel endpoints.
  */
private final class SequencerChannelState(
    val transport: SequencerChannelClientTransport,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  private val endpoints = new mutable.HashMap[SequencerChannelId, SequencerChannelClientEndpoint]()

  def addChannelEndpoint(endpoint: SequencerChannelClientEndpoint): Either[String, Unit] =
    blocking(this.synchronized {
      val channelId = endpoint.channelId
      for {
        _ <- endpoints
          .get(channelId)
          .toLeft(())
          .leftMap(channel => s"Channel ${channel.channelId} endpoint already exists")
        _ = endpoints.put(channelId, endpoint).discard
      } yield ()
    })

  def closeChannelEndpoint(
      channelId: SequencerChannelId
  ): Either[String, Unit] =
    blocking(this.synchronized {
      endpoints.remove(channelId).toRight(s"Channel $channelId not found").map { endpoint =>
        logger.debug(s"About to close endpoint ${endpoint.channelId}")(TraceContext.empty)
        endpoint.close()
      }
    })

  def closeChannelsAndTransport(): Unit = {
    blocking(this.synchronized {
      endpoints.foreach { case (_, endpoint) => endpoint.close() }
    })
    transport.close()
  }
}
