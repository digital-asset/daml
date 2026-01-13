// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_dev

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Synchronize.waitForContract
import com.daml.ledger.test.java.experimental.test.{Asset, Proposal}

final class ContractKeysDivulgenceIT extends LedgerTestSuite {

  test(
    "DivulgenceKeys",
    "Divulgence should behave as expected in a workflow involving keys",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, Seq(proposer)), Participant(beta, Seq(owner))) =>
      for {
        offer <- alpha.create(proposer, new Proposal(proposer, owner))(Proposal.COMPANION)
        asset <- beta.create(owner, new Asset(owner, owner))(Asset.COMPANION)
        _ <- waitForContract(beta, owner, offer)
        _ <- beta.exercise(owner, offer.exerciseProposalAccept(asset))
      } yield {
        // nothing to test, if the workflow ends successfully the test is considered successful
      }
  })

}
