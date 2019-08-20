// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.digitalasset.daml.lf.data.Ref

import scala.concurrent.Future

object LedgerTest {

  def apply(shortIdentifier: String, description: String, timeout: Long = 30000L)(
      test: LedgerTestContext => Future[Unit]): LedgerTest =
    new LedgerTest(Ref.LedgerString.assertFromString(shortIdentifier), description, timeout, test)

}

final class LedgerTest private (
    val shortIdentifier: Ref.LedgerString,
    val description: String,
    val timeout: Long,
    val test: LedgerTestContext => Future[Unit])
    extends (LedgerTestContext => Future[Unit]) {
  override def apply(context: LedgerTestContext): Future[Unit] = test(context)
}
