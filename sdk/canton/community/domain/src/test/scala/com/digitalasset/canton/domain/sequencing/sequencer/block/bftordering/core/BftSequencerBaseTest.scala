// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoFutureUnlessShutdown
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import org.scalatest.Assertion

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

trait BftSequencerBaseTest extends BaseTest {

  protected implicit def toFuture[X](x: PekkoFutureUnlessShutdown[X])(implicit
      ec: ExecutionContext
  ): Future[X] =
    x.futureUnlessShutdown.failOnShutdown(x.action)

  protected final def assertLogs[A](within: => A, assertions: (LogEntry => Assertion)*): A =
    loggerFactory.assertLogs(SuppressionRule.FullSuppression)(within, assertions*)

  protected final def assertNoLogs[A](within: => A): A =
    loggerFactory.assertLogs(SuppressionRule.FullSuppression)(within)

  protected final def suppressProblemLogs[A](within: => A, count: Int = 1): A = {
    val assertions = Seq.fill(count)((_: LogEntry) => succeed)
    loggerFactory.assertLogs(within, assertions*)
  }
}
