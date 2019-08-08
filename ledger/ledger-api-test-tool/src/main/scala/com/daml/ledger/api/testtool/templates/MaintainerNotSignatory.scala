// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.templates

import com.daml.ledger.api.testtool.infrastructure.LedgerTestContext
import com.digitalasset.ledger.api.v1.value.Value.Sum.Party

import scala.concurrent.Future

object MaintainerNotSignatory {
  def apply(p: String, q: String)(
      implicit context: LedgerTestContext): Future[MaintainerNotSignatory] =
    context
      .create(p, ids.maintainerNotSignatory, Map("p" -> Party(p), "q" -> Party(q)))
      .map(new MaintainerNotSignatory(_, p, q) {})
}

sealed abstract case class MaintainerNotSignatory(contractId: String, p: String, q: String)
