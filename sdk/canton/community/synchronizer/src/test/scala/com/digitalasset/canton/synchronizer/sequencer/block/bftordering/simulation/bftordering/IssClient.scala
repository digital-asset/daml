// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.bftordering

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Mempool
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.{
  SimulationClient,
  SimulationSettings,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  Module,
  ModuleRef,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString

import scala.util.Random

class IssClient[E <: Env[E]](
    simSettings: SimulationSettings,
    mempool: ModuleRef[Mempool.OrderRequest],
    name: String,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends Module[E, Unit]
    with NamedLogging {

  import IssClient.*

  private val random = new Random(simSettings.localSettings.randomSeed)

  private var submissionNumber = 0

  override def receiveInternal(msg: Unit)(implicit
      context: E#ActorContextT[Unit],
      traceContext: TraceContext,
  ): Unit = {
    val additionalPayload =
      simSettings.clientRequestApproximateByteSize
        .map(bytes =>
          ByteString.copyFromUtf8("-").concat(ByteString.copyFrom(random.nextBytes(bytes.value)))
        )
        .getOrElse(ByteString.empty)
    val request = context.withNewTraceContext { implicit traceContext =>
      Mempool.OrderRequest(
        Traced(
          OrderingRequest(
            "tag",
            ByteString.copyFromUtf8(s"$name-submission-$submissionNumber").concat(additionalPayload),
          )
        )
      )
    }
    submissionNumber += 1

    mempool.asyncSend(request)

    simSettings.clientRequestInterval.foreach(interval => context.delayedEvent(interval, ()))
  }
}

object IssClient {

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  def initializer[E <: Env[E]](
      simSettings: SimulationSettings,
      node: BftNodeId,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  ): SimulationClient.Initializer[E, Unit, Mempool.Message] =
    new SimulationClient.Initializer[E, Unit, Mempool.Message] {

      override def createClient(systemRef: ModuleRef[Mempool.Message]): Module[E, Unit] =
        new IssClient[E](simSettings, systemRef, node, loggerFactory, timeouts)

      override def init(context: E#ActorContextT[Unit]): Unit =
        // If the interval is None, the progress of the simulation time will solely depend on other delayed events
        // across the BFT Ordering Service (e.g., clock tick events from the Availability module).
        simSettings.clientRequestInterval.foreach(interval => context.delayedEvent(interval, ()))
    }
}
