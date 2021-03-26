// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{ParticipantAllocation, Participants}
import com.daml.lf.data.Ref

import scala.concurrent.{ExecutionContext, Future}

/** @param suite To which collection of tests this case belongs to
  * @param shortIdentifier A unique identifier used to generate party names, command identifiers, etc.
  * @param description A human-readable description of what this case tests
  * @param timeoutScale The factor applied to the default
  * @param runConcurrently True if the test is safe be ran concurrently with other tests without affecting their results
  * @param participants What parties need to be allocated on what participants as a setup for the test case
  * @param runTestCase The body of the test to be executed
  */
sealed class LedgerTestCase(
    val suite: LedgerTestSuite,
    val shortIdentifier: Ref.LedgerString,
    val description: String,
    val timeoutScale: Double,
    val runConcurrently: Boolean,
    val repeated: Int = 1,
    participants: ParticipantAllocation,
    runTestCase: ExecutionContext => Participants => Future[Unit],
) {
  val name: String = s"${suite.name}:$shortIdentifier"
  def apply(context: LedgerTestContext)(implicit ec: ExecutionContext): Future[Unit] =
    context.allocate(participants).flatMap(p => runTestCase(ec)(p))
}
