// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.simulation.bftordering

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.Mempool
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.simulation.SimulationClient
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.{
  Env,
  Module,
  ModuleRef,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString

import scala.concurrent.duration.FiniteDuration

class IssClient[E <: Env[E]](
    requestInterval: Option[FiniteDuration],
    mempool: ModuleRef[Mempool.OrderRequest],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends Module[E, Unit]
    with NamedLogging {
  private var submissionNumber = 0
  override def receiveInternal(msg: Unit)(implicit
      context: E#ActorContextT[Unit],
      traceContext: TraceContext,
  ): Unit = {
    val request = Mempool.OrderRequest(
      Traced(
        OrderingRequest(
          "tag",
          ByteString.copyFromUtf8(f"submission-$submissionNumber"),
        )
      )
    )
    submissionNumber += 1

    mempool.asyncSend(request)

    requestInterval.foreach(interval => context.delayedEvent(interval, ()))
  }
}

object IssClient {
  def initializer[E <: Env[E]](
      requestInterval: Option[FiniteDuration],
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  ): SimulationClient.Initializer[E, Unit, Mempool.Message] =
    new SimulationClient.Initializer[E, Unit, Mempool.Message] {

      override def createClient(systemRef: ModuleRef[Mempool.Message]): Module[E, Unit] =
        new IssClient[E](requestInterval, systemRef, loggerFactory, timeouts)

      override def init(context: E#ActorContextT[Unit]): Unit =
        // If the interval is None, the progress of the simulation time will solely depend on other delayed events
        // across the BFT Ordering Service (e.g., clock tick events from the Availability module).
        requestInterval.foreach(interval => context.delayedEvent(interval, ()))
    }
}
