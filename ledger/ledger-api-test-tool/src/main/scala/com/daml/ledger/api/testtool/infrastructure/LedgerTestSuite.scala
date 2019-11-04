// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite.SkipTestException

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

private[testtool] abstract class LedgerTestSuite(val session: LedgerSession) {
  val name: String = getClass.getSimpleName

  private val testBuffer: ListBuffer[LedgerTest] = ListBuffer()

  final lazy val tests: Vector[LedgerTest] = testBuffer.toVector

  protected implicit final val ec: ExecutionContext = session.executionContext

  protected final def test(shortIdentifier: String, description: String, timeout: Long = 30000L)(
      testCase: LedgerTestContext => Future[Unit]): Unit = {
    testBuffer.append(LedgerTest(shortIdentifier, description, timeout)(testCase))
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
