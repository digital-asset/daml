// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.{Member, SequencerId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Sequencer channel client for interacting with GRPC-based sequencer channels that exchange data with other nodes
  * bypassing the regular canton protocol and ordering-based sequencer service.
  */
class SequencerChannelClient(
    member: Member,
    transportMap: NonEmpty[Map[SequencerId, SequencerChannelClientTransport]],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {
  private[client] val state = new SequencerChannelClientState(transportMap, timeouts, loggerFactory)

  def ping(sequencerId: SequencerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = for {
    transport <- EitherT.fromEither[FutureUnlessShutdown](state.transport(sequencerId))
    _ <- transport.ping()
  } yield ()

  override protected def onClosed(): Unit = state.close()
}
