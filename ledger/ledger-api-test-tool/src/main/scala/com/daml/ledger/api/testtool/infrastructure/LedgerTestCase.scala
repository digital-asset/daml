// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{ParticipantAllocation, Participants}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
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
    val participants: ParticipantAllocation,
    runTestCase: ExecutionContext => Seq[ParticipantTestContext] => Participants => Future[Unit],
) {
  val name: String = s"${suite.name}:$shortIdentifier"

  def apply(context: LedgerTestContext)(implicit ec: ExecutionContext): Future[Unit] =
    context.allocate(participants).flatMap(p => runTestCase(ec)(context.configuredParticipants)(p))

  def repetitions: Vector[LedgerTestCase.Repetition] =
    if (repeated == 1)
      Vector(new LedgerTestCase.Repetition(this, repetition = None))
    else
      (1 to repeated)
        .map(i => new LedgerTestCase.Repetition(this, repetition = Some(i -> repeated)))
        .toVector
}

object LedgerTestCase {
  final class Repetition(val testCase: LedgerTestCase, val repetition: Option[(Int, Int)]) {
    def suite: LedgerTestSuite = testCase.suite
    def shortIdentifier: Ref.LedgerString = testCase.shortIdentifier
    def description: String = testCase.description
    def timeoutScale: Double = testCase.timeoutScale

    def apply(context: LedgerTestContext)(implicit ec: ExecutionContext): Future[Unit] =
      testCase(context)
  }
}
