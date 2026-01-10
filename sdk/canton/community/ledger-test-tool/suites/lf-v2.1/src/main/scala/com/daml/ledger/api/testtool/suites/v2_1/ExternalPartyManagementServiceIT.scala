// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.v2.admin.party_management_service.GetPartiesRequest

class ExternalPartyManagementServiceIT extends PartyManagementITBase {
  test(
    "PMAllocateExternalPartyBasic",
    "Allocate an external party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    for {
      party <- ledger.allocateExternalPartyFromHint(Some("alice"))
      get <- ledger.getParties(
        GetPartiesRequest(parties = Seq(party), identityProviderId = "")
      )
    } yield {
      assert(
        party.getValue.contains("alice"),
        "The allocated party identifier does not contain the party hint",
      )
      assertDefined(
        get.partyDetails.find(_.party == party.getValue),
        "Expected to list allocated party",
      )
    }
  })
}
