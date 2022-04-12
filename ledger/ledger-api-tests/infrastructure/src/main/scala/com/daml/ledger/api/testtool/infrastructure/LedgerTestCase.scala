// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{Participants, PartyAllocation}
import com.daml.ledger.api.testtool.infrastructure.participant.{Features, ParticipantTestContext}
import com.daml.lf.data.Ref
import com.daml.test.evidence.tag.EvidenceTag

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** @param suite          To which collection of tests this case belongs to
  * @param shortIdentifier A unique identifier used to generate party names, command identifiers, etc.
  * @param description     A human-readable description of what this case tests
  * @param timeoutScale    The factor applied to the default
  * @param runConcurrently True if the test is safe be ran concurrently with other tests without affecting their results
  * @param partyAllocation    What parties need to be allocated on what participants as a setup for the test case
  * @param runTestCase     The body of the test to be executed
  */
sealed class LedgerTestCase(
    val suite: LedgerTestSuite,
    val shortIdentifier: Ref.LedgerString,
    val description: String,
    val timeoutScale: Double,
    val runConcurrently: Boolean,
    val repeated: Int = 1,
    val tags: List[EvidenceTag] = List.empty,
    enabled: Features => Boolean,
    disabledReason: String,
    partyAllocation: PartyAllocation,
    runTestCase: ExecutionContext => Seq[ParticipantTestContext] => Participants => Future[Unit],
) {
  val name: String = s"${suite.name}:$shortIdentifier"

  private def allocatePartiesAndRun(
      context: LedgerTestContext
  )(implicit ec: ExecutionContext): Future[Unit] = {
    for {
      participants: Participants <- context.allocateParties(partyAllocation)
      result <- runTestCase(ec)(context.configuredParticipants)(participants)
        .transformWith { result =>
          cleanUpCreatedUsers(context, result)
        }
    } yield {
      result
    }
  }

  def repetitions: Vector[LedgerTestCase.Repetition] =
    if (repeated == 1)
      Vector(new LedgerTestCase.Repetition(this, repetition = None))
    else
      (1 to repeated)
        .map(i => new LedgerTestCase.Repetition(this, repetition = Some(i -> repeated)))
        .toVector

  def isEnabled(features: Features, participantCount: Int): Either[String, Unit] =
    for {
      _ <- Either.cond(enabled(features), (), disabledReason)
      _ <- Either.cond(
        partyAllocation.minimumParticipantCount <= participantCount,
        (),
        "Not enough participants to run this test case.",
      )
    } yield ()

  /** Deletes users created during this test case execution.
    */
  private def cleanUpCreatedUsers(context: LedgerTestContext, testCaseRunResult: Try[Unit])(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    lazy val deleteCreatedUsersF =
      Future.sequence(context.configuredParticipants.map(_.deleteCreatedUsers()))
    testCaseRunResult match {
      case Success(v) => deleteCreatedUsersF.map(_ => v)
      case Failure(exception) =>
        // Prioritizing a failure of users' clean-up over the original failure of the test case
        // since clean-up failures can affect other test cases.
        deleteCreatedUsersF.flatMap(_ => Future.failed(exception))
    }
  }

}

object LedgerTestCase {

  final class Repetition(val testCase: LedgerTestCase, val repetition: Option[(Int, Int)]) {
    def suite: LedgerTestSuite = testCase.suite

    def shortIdentifier: Ref.LedgerString = testCase.shortIdentifier

    def description: String = testCase.description

    def timeoutScale: Double = testCase.timeoutScale

    def allocatePartiesAndRun(context: LedgerTestContext)(implicit
        ec: ExecutionContext
    ): Future[Unit] =
      testCase.allocatePartiesAndRun(context)
  }

}
