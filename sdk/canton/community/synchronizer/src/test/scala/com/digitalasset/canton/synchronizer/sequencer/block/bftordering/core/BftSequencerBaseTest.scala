// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoFutureUnlessShutdown
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.Assertion

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

trait BftSequencerBaseTest extends BaseTest {

  protected final implicit lazy val synchronizerProtocolVersion: ProtocolVersion =
    testedProtocolVersion

  protected implicit def toFuture[X](x: PekkoFutureUnlessShutdown[X])(implicit
      ec: ExecutionContext
  ): Future[X] =
    x.futureUnlessShutdown().failOnShutdown(x.action)

  protected final def assertLogs[A](within: => A, assertions: (LogEntry => Assertion)*): A =
    loggerFactory.assertLogs(SuppressionRule.FullSuppression)(within, assertions*)

  protected final def assertNoLogs[A](within: => A): A =
    loggerFactory.assertLogs(SuppressionRule.FullSuppression)(within)

  protected final def suppressProblemLogs[A](within: => A, count: Int = 1): A = {
    val assertions = Seq.fill(count)((_: LogEntry) => succeed)
    loggerFactory.assertLogs(within, assertions*)
  }
}

object BftSequencerBaseTest {
  implicit class FakeSigner[A <: ProtocolVersionedMemoizedEvidence & MessageFrom](msg: A) {
    def fakeSign: SignedMessage[A] = SignedMessage(msg, Signature.noSignature)
  }
}
