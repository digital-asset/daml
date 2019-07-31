// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool.infrastructure

import scala.concurrent.Future

final case class LedgerTest(description: String)(test: LedgerTestContext => Future[Unit])
    extends (LedgerTestContext => Future[Unit]) {
  val timeout: Long = 30000L
  override def apply(context: LedgerTestContext): Future[Unit] = test(context)
}
