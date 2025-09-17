// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.participant.admin.party.PartyReplicationTestInterceptor
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, SimpleExecutionQueue}

import scala.util.chaining.scalaUtilChainingOps

trait PartyReplicationProcessor extends SequencerChannelProtocolProcessor {
  def replicatedContractsCount: NonNegativeInt

  protected def name: String
  protected def futureSupervisor: FutureSupervisor
  protected def exitOnFatalFailures: Boolean

  protected def hasEndOfACSBeenReached: Boolean

  protected def isChannelOpenForCommunication: Boolean = isChannelConnected && !hasChannelCompleted

  protected def processorStore: PartyReplicationProcessorStore

  protected val executionQueue = new SimpleExecutionQueue(
    name,
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  protected def testOnlyInterceptor: PartyReplicationTestInterceptor

  protected def onAcsFullyReplicated: TraceContext => Unit
  protected def onError: String => Unit
  protected def onDisconnect: (String, TraceContext) => Unit

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
      eitherT.valueOr(err => logger.warn(s"\"$operation\" failed with $err"))

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

  /** Let the PartyReplication know that the processor has gotten disconnected from the sequencer
    * channel endpoint. Only notify if the disconnect has actually triggered a transition losing the
    * endpoint and unless ACS replication has already completed (in which case the disconnect
    * happens precisely because OnPR has completed).
    * @return
    *   true iff the channel endpoint has been lost
    */
  override def onDisconnected(status: Either[String, Unit])(implicit
      traceContext: TraceContext
  ): Boolean = {
    // Clear the initial contract ordinal so the TP remembers to reinitialize the SP
    // in case we reconnect.
    processorStore.clearInitialContractOrdinalInclusive()

    super
      .onDisconnected(status)
      .tap(hasLostChannelEndpoint =>
        if (hasLostChannelEndpoint && !hasEndOfACSBeenReached) {
          logger.info("Channel endpoint has been disconnected before ACS replication completion")
          onDisconnect(
            status.swap.getOrElse("Channel endpoint has been disconnected"),
            traceContext,
          )
        }
      )
  }

  override def onClosed(): Unit = LifeCycle.close(executionQueue)(logger)
}
