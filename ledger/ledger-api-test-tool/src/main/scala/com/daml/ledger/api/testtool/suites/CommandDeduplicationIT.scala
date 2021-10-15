// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.deduplication.CommandDeduplicationBase
import com.daml.ledger.api.testtool.infrastructure.deduplication.CommandDeduplicationBase.DeduplicationFeatures
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.timer.Delayed

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/** Command deduplication tests for participant side deduplication
  * Should be disabled for ledgers that have committer side deduplication enabled (KV)
  */
final class CommandDeduplicationIT(
    timeoutScaleFactor: Double,
    ledgerTimeInterval: FiniteDuration,
    staticTime: Boolean,
) extends CommandDeduplicationBase(timeoutScaleFactor, ledgerTimeInterval, staticTime) {

  override def runWithDelay(
      participants: Seq[ParticipantTestContext]
  )(test: (() => Future[Unit]) => Future[Unit])(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    test(() => Delayed.by(defaultDeduplicationWindowWait)(()))
  }

  override def testNamingPrefix: String = "ParticipantCommandDeduplication"

  override def deduplicationFeatures: CommandDeduplicationBase.DeduplicationFeatures =
    DeduplicationFeatures(
      participantDeduplication = true,
      appendOnlySchema = false,
    )
}
