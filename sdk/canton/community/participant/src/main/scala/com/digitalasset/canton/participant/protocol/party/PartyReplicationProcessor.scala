// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, SimpleExecutionQueue}

import java.util.concurrent.atomic.AtomicBoolean

trait PartyReplicationProcessor extends SequencerChannelProtocolProcessor {
  def replicatedContractsCount: NonNegativeInt

  protected def name: String
  protected def futureSupervisor: FutureSupervisor
  protected def exitOnFatalFailures: Boolean

  protected val hasEndOfACSBeenReached = new AtomicBoolean(false)
  protected val isChannelClosed = new AtomicBoolean(false)

  protected val executionQueue = new SimpleExecutionQueue(
    name,
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  /** Single point of entry for progress monitoring and advancing party replication.
    */
  def progressPartyReplication()(implicit traceContext: TraceContext): Unit

  final protected def executeAsync(operation: String)(
      code: => EitherT[FutureUnlessShutdown, String, Unit]
  )(implicit traceContext: TraceContext): Unit = {
    def logError(
        eitherT: EitherT[
          FutureUnlessShutdown,
          String,
          Unit,
        ]
    ): FutureUnlessShutdown[Unit] =
      eitherT.valueOr(err => logger.warn(s"$operation failed with $err"))

    logger.debug(s"About to $operation")
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      logError(execute[Unit](operation)(code)),
      s"$operation failed",
    )
  }

  final protected def execute[T](operation: String)(
      code: => EitherT[FutureUnlessShutdown, String, T]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, T] =
    executionQueue.executeEUS[String, T](code, operation)
}
