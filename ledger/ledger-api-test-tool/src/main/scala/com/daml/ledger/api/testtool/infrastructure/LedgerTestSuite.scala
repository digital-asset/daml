// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite.SkipTestException

import scala.concurrent.{ExecutionContext, Future}

private[testtool] abstract class LedgerTestSuite(val session: LedgerSession) {
  val name: String = getClass.getSimpleName

  val tests: Vector[LedgerTest]

  protected implicit final val ec: ExecutionContext = session.executionContext

  final def skip(reason: String): Future[Unit] = Future.failed(SkipTestException(reason))

  final def skipIf(reason: String)(p: => Boolean): Future[Unit] =
    if (p) skip(reason) else Future.successful(())
}

private[testtool] object LedgerTestSuite {
  final case class SkipTestException(message: String) extends RuntimeException(message)
}
