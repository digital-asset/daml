// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.infrastructure

import scala.concurrent.Future

final case class LedgerTest(description: String)(test: LedgerTestContext => Future[Unit])
    extends (LedgerTestContext => Future[Unit]) {
  override final def apply(context: LedgerTestContext): Future[Unit] = test(context)
}
