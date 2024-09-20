// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.SequencerId

import scala.concurrent.{Future, Promise}
import scala.util.Success

/** SequencerChannelClientState holds the set of (static) channel transports and per-sequencer channel state.
  */
final class SequencerChannelClientState(
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

  // Promised completed upon sequencer channel client shutdown or
  // TODO(#21120) when an unexpected channel error occurs.
  private val closePromise = Promise[Unit]()

  def completion: Future[Unit] = closePromise.future

  private def closeChannelsAndTransports(): Unit = {
    transports.toList.foreach { case (_, consumerState) =>
      consumerState.closeChannelsAndTransport()
    }

    closePromise.tryComplete(Success(())).discard
  }

  override protected def onClosed(): Unit =
    closeChannelsAndTransports()
}

/** SequencerChannelState holds the sequencer-specific channel transport
  * and manages the set of dynamically changing channel endpoints.
  */
private final class SequencerChannelState(
    val transport: SequencerChannelClientTransport,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def closeChannelsAndTransport(): Unit =
    transport.close()
}
