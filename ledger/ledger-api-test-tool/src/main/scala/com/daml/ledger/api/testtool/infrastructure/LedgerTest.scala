// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import scala.concurrent.Future

object LedgerTest {

  def apply(description: String, timeout: Long = 30000L)(
      test: LedgerTestContext => Future[Unit]): LedgerTest =
    new LedgerTest(description, timeout, test)

}

final class LedgerTest private (
    val description: String,
    val timeout: Long,
    val test: LedgerTestContext => Future[Unit])
    extends (LedgerTestContext => Future[Unit]) {
  override def apply(context: LedgerTestContext): Future[Unit] = test(context)
}
