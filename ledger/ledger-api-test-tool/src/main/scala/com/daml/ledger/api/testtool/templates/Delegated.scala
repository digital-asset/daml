// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.templates

import com.daml.ledger.api.testtool.infrastructure.LedgerTestContext
import com.digitalasset.ledger.api.v1.value.Value.Sum.{Party, Text}

import scala.concurrent.Future

object Delegated {
  def apply(owner: String, k: String)(implicit context: LedgerTestContext): Future[Delegated] =
    context
      .create(owner, ids.delegated, Map("owner" -> Party(owner), "k" -> Text(k)))
      .map(new Delegated(_, owner, k) {})
}

sealed abstract case class Delegated(contractId: String, owner: String, k: String)
