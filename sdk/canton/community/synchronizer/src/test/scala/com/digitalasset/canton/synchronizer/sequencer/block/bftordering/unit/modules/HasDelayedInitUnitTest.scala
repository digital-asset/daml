// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.HasDelayedInit
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Module,
  ModuleRef,
}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AnyWordSpec

class HasDelayedInitUnitTest extends AnyWordSpec with BaseTest {
  private implicit val unitTestContext: UnitTestContext[UnitTestEnv, String] =
    UnitTestContext()

  "a delayed-init module" when {
    "receiving messages" should {
      "postpone processing them until init is completed" in {
        val echoRef = mock[ModuleRef[String]]

        val delayedInitModule = new DelayedInitModule(echoRef, loggerFactory, timeouts)
        delayedInitModule.receive("message1")

        verify(echoRef, never).asyncSend("message1")

        delayedInitModule.receive("init")

        verify(echoRef, times(1)).asyncSend(eqTo("initComplete"))
        verify(echoRef, times(1)).asyncSend(eqTo("message1"))

        delayedInitModule.receive("message2")

        verify(echoRef, times(1)).asyncSend(eqTo("message2"))
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
