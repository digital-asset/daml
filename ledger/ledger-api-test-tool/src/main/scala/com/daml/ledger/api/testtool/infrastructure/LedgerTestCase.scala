// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{ParticipantAllocation, Participants}
import com.daml.lf.data.Ref

import scala.concurrent.{ExecutionContext, Future}

final class LedgerTestCase(
    val shortIdentifier: Ref.LedgerString,
    val description: String,
    val timeoutScale: Double,
    participants: ParticipantAllocation,
    runTestCase: Participants => Future[Unit],
) {
  def apply(context: LedgerTestContext)(implicit ec: ExecutionContext): Future[Unit] =
    context.allocate(participants).flatMap(runTestCase)
}
