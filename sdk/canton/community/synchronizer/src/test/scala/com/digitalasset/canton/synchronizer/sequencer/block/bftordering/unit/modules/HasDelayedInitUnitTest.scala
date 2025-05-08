// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.HasDelayedInit
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Module,
  ModuleRef,
}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AnyWordSpec

class HasDelayedInitUnitTest extends AnyWordSpec with BftSequencerBaseTest {
  private implicit val unitTestContext: UnitTestContext[UnitTestEnv, String] =
    UnitTestContext()

  "a delayed-init module" when {
    "receiving messages" should {
      "postpone processing them until init is completed" in {
        val echoRef = mock[ModuleRef[String]]

        val delayedInitModule = new DelayedInitModule(echoRef, loggerFactory, timeouts)
        delayedInitModule.receive("message1")

        verify(echoRef, never).asyncSend(eqTo("message1"))(any[MetricsContext])

        delayedInitModule.receive("init")

        verify(echoRef, times(1)).asyncSend(eqTo("initComplete"))(any[MetricsContext])
        verify(echoRef, times(1)).asyncSend(eqTo("message1"))(any[MetricsContext])

        delayedInitModule.receive("message2")

        verify(echoRef, times(1)).asyncSend(eqTo("message2"))(any[MetricsContext])
      }
    }
  }
}

class DelayedInitModule(
    echoRef: ModuleRef[String],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends Module[UnitTestEnv, String]
    with HasDelayedInit[String] {

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  override def receiveInternal(
      message: String
  )(implicit context: UnitTestContext[UnitTestEnv, String], traceContext: TraceContext): Unit =
    message match {
      case "init" =>
        echoRef.asyncSend("initComplete")
        initCompleted(receiveInternal)
      case _ =>
        ifInitCompleted(message)(echoRef.asyncSend)
    }
}
