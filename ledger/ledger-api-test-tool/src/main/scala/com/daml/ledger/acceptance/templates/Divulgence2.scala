// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.templates

import com.daml.ledger.acceptance.infrastructure.LedgerTestContext
import com.digitalasset.ledger.api.v1.value.Value.Sum.{ContractId, Party}

import scala.concurrent.Future

object Divulgence2 {
  def apply(party: String, div2Signatory: String, div2Fetcher: String)(
      implicit context: LedgerTestContext): Future[Divulgence2] =
    context
      .create(
        party,
        ids.divulgence2,
        Map("div2Signatory" -> new Party(div2Signatory), "div2Fetcher" -> new Party(div2Fetcher))
      )
      .map(new Divulgence2(_, party, div2Signatory, div2Fetcher) {})

  sealed abstract case class Divulgence2(
      contractId: String,
      party: String,
      div2Signatory: String,
      div2Fetcher: String) {
    def archive(party: String, div1ToArchive: Divulgence1)(
        implicit context: LedgerTestContext): Future[Unit] =
      context.exercise(
        party,
        ids.divulgence2,
        contractId,
        "Divulgence2Archive",
        Map(
          "div1ToArchive" -> new ContractId(div1ToArchive.contractId)
        )
      )
  }

}
