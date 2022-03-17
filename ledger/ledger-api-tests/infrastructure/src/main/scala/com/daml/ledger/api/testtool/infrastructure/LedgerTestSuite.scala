// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{Participants, PartyAllocation}
import com.daml.ledger.api.testtool.infrastructure.participant.{Features, ParticipantTestContext}
import com.daml.lf.data.Ref
import com.daml.test.evidence.tag.EvidenceTag

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

abstract class LedgerTestSuite {
  private val testCaseBuffer: ListBuffer[LedgerTestCase] = ListBuffer()

  final lazy val tests: Vector[LedgerTestCase] = testCaseBuffer.toVector

  protected final def test(
      shortIdentifier: String,
      description: String,
      partyAllocation: PartyAllocation,
      timeoutScale: Double = 1.0,
      runConcurrently: Boolean = true,
      repeated: Int = 1,
      enabled: Features => Boolean = _ => true,
      disabledReason: String = "No reason",
      tags: List[EvidenceTag] = List.empty,
  )(testCase: ExecutionContext => PartialFunction[Participants, Future[Unit]]): Unit = {
    testGivenAllParticipants(
      shortIdentifier,
      description,
      partyAllocation,
      timeoutScale,
      runConcurrently,
      repeated,
      enabled,
      disabledReason,
      tags,
    )((ec: ExecutionContext) => (_: Seq[ParticipantTestContext]) => testCase(ec))
  }

  protected final def testGivenAllParticipants(
      shortIdentifier: String,
      description: String,
      partyAllocation: PartyAllocation,
      timeoutScale: Double = 1.0,
      runConcurrently: Boolean = true,
      repeated: Int = 1,
      enabled: Features => Boolean = _ => true,
      disabledReason: String = "No reason",
      tags: List[EvidenceTag] = List.empty,
  )(
      testCase: ExecutionContext => Seq[ParticipantTestContext] => PartialFunction[
        Participants,
        Future[Unit],
      ]
  ): Unit = {
    val shortIdentifierRef = Ref.LedgerString.assertFromString(shortIdentifier)
    testCaseBuffer.append(
      new LedgerTestCase(
        this,
        shortIdentifierRef,
        description,
        timeoutScale,
        runConcurrently,
        repeated,
        tags,
        enabled,
        disabledReason,
        partyAllocation,
        testCase,
      )
    )
  }

  def name: String = getClass.getSimpleName
}
