// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import better.files.File
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{AcsInspection, PartyToParticipantDeclarative}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP

/** Consider the following setup:
  *   - Alice, Bob hosted on participant1
  *   - participant2 not hosting any party
  *   - Alice and Bob are stakeholders on a contract
  *
  * We check that we can first replicate Alice and then Bob. The reason for this that is that it was
  * a limitation in 2.x
  */
trait OfflinePartyReplicationSharedContractIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection {

  private val aliceName = "Alice"
  private val bobName = "Bob"

  private var alice: PartyId = _
  private var bob: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*

      sequencer1.topology.synchronizer_parameters.propose_update(
        synchronizerId = daId,
        _.update(reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)),
      )

      participants.all.synchronizers.connect_local(sequencer1, alias = daName)

      participants.all.dars.upload(CantonExamplesPath)

      alice = participant1.parties.enable(aliceName)
      bob = participant1.parties.enable(bobName)
    }

  private def replicateParty(
      party: PartyId
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val ledgerEndP1 = participant1.ledger_api.state.end()

    PartyToParticipantDeclarative.forParty(Set(participant1, participant2), daId)(
      participant1,
      party,
      PositiveInt.one,
      Set(
        (participant1, PP.Submission),
        (participant2, PP.Observation),
      ),
    )

    val partyAddedOnP2Offset = participant1.parties
      .find_party_max_activation_offset(
        partyId = party,
        participantId = participant2.id,
        synchronizerId = daId,
        beginOffsetExclusive = ledgerEndP1,
        completeAfter = PositiveInt.one,
      )

    val file = File.newTemporaryFile(s"acs_export_chopper_$party")
    participant1.parties.export_acs(
      Set(party),
      exportFilePath = file.canonicalPath,
      ledgerOffset = partyAddedOnP2Offset,
    )

    participant2.synchronizers.disconnect_all()
    participant2.repair.import_acs(file.canonicalPath)
    participant2.synchronizers.reconnect_all()
  }

  "check that stakeholders of a contract can be replicated to the same target participant" in {
    implicit env =>
      import env.*

      IouSyntax.createIou(participant1)(alice, bob)

      participant1.ledger_api.state.acs.of_party(alice) should have size 1
      participant1.ledger_api.state.acs.of_party(bob) should have size 1
      participant2.ledger_api.state.acs.of_party(alice) should have size 0

      replicateParty(alice)
      participant2.ledger_api.state.acs.of_party(alice) should have size 1
      participant2.ledger_api.state.acs.of_party(bob) should have size 1

      replicateParty(bob)
      participant2.ledger_api.state.acs.of_party(alice) should have size 1
      participant2.ledger_api.state.acs.of_party(bob) should have size 1
  }
}

class OfflinePartyReplicationSharedContractIntegrationTestPostgres
    extends OfflinePartyReplicationSharedContractIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
