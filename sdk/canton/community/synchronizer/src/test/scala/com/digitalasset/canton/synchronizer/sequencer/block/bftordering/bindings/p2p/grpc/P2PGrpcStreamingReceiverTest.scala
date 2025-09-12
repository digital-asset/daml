// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.lifecycle.PromiseUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.topology.{Namespace, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext

class P2PGrpcStreamingReceiverTest extends AnyWordSpec with BftSequencerBaseTest {

  // Direct execution context is crucial for the tests to verify entire code paths.
  implicit private val executionContext: ExecutionContext = DirectExecutionContext(logger)

  "P2PGrpcStreamingReceiver" should {
    "throw an exception if received gRPC message before authentication was completed" in {
      val message =
        BftOrderingMessage(traceContext = "", body = None, sentBy = "node1", sentAt = None)
      val inputModule = mock[ModuleRef[BftOrderingMessage]]
      val sequencerIdPromiseUS = PromiseUnlessShutdown.unsupervised[SequencerId]()
      val receiver = createStreamingReceiver(inputModule, sequencerIdPromiseUS)

      // Receive a gRPC message before authentication was completed.
      a[RuntimeException] shouldBe thrownBy(suppressProblemLogs(receiver.onNext(message)))
    }

    "forward a message if node ID validation was successful" in {
      val sequencerId =
        SequencerId.tryCreate("node1", Namespace(Fingerprint.tryFromString("namespace")))
      val message =
        BftOrderingMessage(
          traceContext = "",
          body = None,
          sentBy = SequencerNodeId.toBftNodeId(sequencerId),
          sentAt = None,
        )
      val inputModule = mock[ModuleRef[BftOrderingMessage]]
      val sequencerIdPromiseUS = PromiseUnlessShutdown.unsupervised[SequencerId]()
      sequencerIdPromiseUS.outcome(sequencerId)
      val receiver = createStreamingReceiver(inputModule, sequencerIdPromiseUS)

      receiver.onNext(message)

      verify(inputModule, times(1)).asyncSend(eqTo(message))(any[TraceContext], any[MetricsContext])
    }

    "not forward a message when node ID validation was not successful" in {
      val message =
        BftOrderingMessage(traceContext = "", body = None, sentBy = "wrong", sentAt = None)
      val inputModule = mock[ModuleRef[BftOrderingMessage]]
      val sequencerIdPromiseUS = PromiseUnlessShutdown.unsupervised[SequencerId]()
      sequencerIdPromiseUS.outcome(
        SequencerId.tryCreate("node1", Namespace(Fingerprint.tryFromString("namespace")))
      )
      val receiver = createStreamingReceiver(inputModule, sequencerIdPromiseUS)

      assertLogs(
        receiver.onNext(message),
        (logEntry: LogEntry) => {
          logEntry.level shouldBe Level.WARN
          logEntry.message should include(
            "Sequencer ID (`SEQ::node1::namespace`) from authentication and `sentBy` (`wrong`) do not match in gRPC message"
          )
        },
        (logEntry: LogEntry) => {
          logEntry.level shouldBe Level.INFO
          logEntry.message should include("Received error")
        },
      )

      verify(inputModule, never).asyncSend(eqTo(message))(any[TraceContext], any[MetricsContext])
    }
  }

  private def createStreamingReceiver(
      inputModule: ModuleRef[BftOrderingMessage],
      sequencerIdPromiseUS: PromiseUnlessShutdown[SequencerId],
  ): P2PGrpcStreamingReceiver = {
    val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
    new P2PGrpcStreamingReceiver(
      maybeP2PEndpointId = None,
      inputModule,
      sequencerIdPromiseUS,
      isAuthenticationEnabled = true,
      metrics,
      loggerFactory,
    ) {
      override def shutdown(): Unit = ()
    }
  }
}
