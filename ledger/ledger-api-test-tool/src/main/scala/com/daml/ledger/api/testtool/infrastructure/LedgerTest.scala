// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.digitalasset.daml.lf.data.Ref

import scala.concurrent.Future

final class LedgerTest(
    val shortIdentifier: Ref.LedgerString,
    val description: String,
    val timeout: Long,
    test: LedgerTestContext => Future[Unit]) {
  def apply(context: LedgerTestContext): Future[Unit] = test(context)
}
