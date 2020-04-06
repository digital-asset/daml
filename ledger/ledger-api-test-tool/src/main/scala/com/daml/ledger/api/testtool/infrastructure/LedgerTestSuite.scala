// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.Allocation.{ParticipantAllocation, Participants}
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite._
import com.daml.lf.data.Ref

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

private[testtool] abstract class LedgerTestSuite(val session: LedgerSession) {
  val name: String = getClass.getSimpleName

  private val testCaseBuffer: ListBuffer[LedgerTestCase] = ListBuffer()

  final lazy val tests: Vector[LedgerTestCase] = testCaseBuffer.toVector

  protected implicit final val ec: ExecutionContext = session.executionContext

  protected final def test(
      shortIdentifier: String,
      description: String,
      participants: ParticipantAllocation,
      timeoutScale: Double = 1.0,
  )(testCase: Participants => Future[Unit]): Unit = {
    val shortIdentifierRef = Ref.LedgerString.assertFromString(shortIdentifier)
    testCaseBuffer.append(
      new LedgerTestCase(shortIdentifierRef, description, timeoutScale, participants, testCase),
    )
  }

  protected final def skip(reason: String): Future[Unit] = Future.failed(SkipTestException(reason))

  protected final def skipIf(reason: String)(p: => Boolean): Future[Unit] =
    if (p)
      skip(reason)
    else
      Future.successful(())
}

private[testtool] object LedgerTestSuite {
  final case class SkipTestException(message: String) extends RuntimeException(message)
}
