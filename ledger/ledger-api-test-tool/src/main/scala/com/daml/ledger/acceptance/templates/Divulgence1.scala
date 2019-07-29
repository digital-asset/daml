// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.templates

import com.daml.ledger.acceptance.infrastructure.LedgerTestContext
import com.digitalasset.ledger.api.v1.value.Value.Sum.Party

import scala.concurrent.Future

object Divulgence1 {
  def apply(issuer: String, div1Party: String)(
      implicit context: LedgerTestContext): Future[Divulgence1] =
    context
      .create(issuer, ids.divulgence1, Map("div1Party" -> new Party(div1Party)))
      .map(new Divulgence1(_, issuer, div1Party) {})
}

sealed abstract case class Divulgence1(contractId: String, issuer: String, div1Party: String) {}
