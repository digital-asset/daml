// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{ParticipantAllocation, Participants}
import com.digitalasset.daml.lf.data.Ref

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

final class LedgerTestCase(
    val shortIdentifier: Ref.LedgerString,
    val description: String,
    val timeout: Duration,
    participants: ParticipantAllocation,
    runTestCase: Participants => Future[Unit],
) {
  def apply(context: LedgerTestContext)(implicit ec: ExecutionContext): Future[Unit] =
    context.allocate(participants).flatMap(runTestCase)
}
