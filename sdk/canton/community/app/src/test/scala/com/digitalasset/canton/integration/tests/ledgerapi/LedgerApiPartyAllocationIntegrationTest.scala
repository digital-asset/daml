// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.participant.sync.SyncServiceError.PartyAllocationNoSynchronizerError

trait LedgerApiPartyAllocationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1

  "allocate party without synchronizer fails" in { implicit env =>
    import env.*

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.ledger_api.parties.allocate("bob"),
      _.errorMessage should include(PartyAllocationNoSynchronizerError.id),
    )
  }

  "a participant knows their admin party" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    val bob = participant1.parties.enable("bob")

    eventually() {
      // eventually bob's party allocation will be published to the synchronizer,
      // together with the automatic package vetting of the admin workflows
      participant1.ledger_api.parties.list().map(_.party) should contain allElementsOf List(
        bob,
        participant1.id.adminParty,
      )
    }
  }

  "reject a double party allocation" in { implicit env =>
    import env.*

    participant1.ledger_api.parties.allocate("alice")
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.ledger_api.parties.allocate("alice"),
      x => {
        x.shouldBeCantonErrorCode(InvalidArgument)
        x.errorMessage should include regex "Party already exists: party .* is already allocated on this node"
      },
    )
  }

}

//class LedgerApiPartyAllocationIntegrationTestDefault extends LedgerApiPartyAllocationIntegrationTest {
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
//}

class LedgerApiPartyAllocationIntegrationTestPostgres
    extends LedgerApiPartyAllocationIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
