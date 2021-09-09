// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{ParticipantAllocation, Participants}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.lf.data.Ref

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

private[testtool] abstract class LedgerTestSuite {
  private val testCaseBuffer: ListBuffer[LedgerTestCase] = ListBuffer()

  final lazy val tests: Vector[LedgerTestCase] = testCaseBuffer.toVector

  protected final def test(
      shortIdentifier: String,
      description: String,
      participants: ParticipantAllocation,
      timeoutScale: Double = 1.0,
      runConcurrently: Boolean = true,
      repeated: Int = 1,
  )(testCase: ExecutionContext => PartialFunction[Participants, Future[Unit]]): Unit = {
    testGivenAllParticipants(
      shortIdentifier,
      description,
      participants,
      timeoutScale,
      runConcurrently,
      repeated,
    )((ec: ExecutionContext) => (_: Seq[ParticipantTestContext]) => testCase(ec))
  }
  protected final def testGivenAllParticipants(
      shortIdentifier: String,
      description: String,
      participants: ParticipantAllocation,
      timeoutScale: Double = 1.0,
      runConcurrently: Boolean = true,
      repeated: Int = 1,
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
        participants,
        testCase,
      )
    )
  }

  private[testtool] def name: String = getClass.getSimpleName
}
