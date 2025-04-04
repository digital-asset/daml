// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.admin.api.client.data.PartyDetails
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.console.commands.SynchronizerChoice
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.PartyManagementServiceErrors.PartyNotFound
import com.digitalasset.canton.topology.TopologyManagerError.MappingAlreadyExists
import com.digitalasset.canton.topology.transaction.{ParticipantPermission, TopologyChangeOp}
import com.digitalasset.canton.topology.{PartyId, SynchronizerId, UniqueIdentifier}

import java.util.UUID

trait PartyManagementIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant2.synchronizers.connect_local(sequencer1, alias = daName)
    }

  "parties group" when {
    "adding parties" should {
      "allow adding 100" in { implicit env =>
        import env.*

        participant2.parties.enable("LordSandwich")
        (1 to 100).foreach { idx =>
          participant1.parties.enable(s"myparty$idx")
        }
        eventually() {
          participant1.parties
            .list(filterParty = "myparty", limit = 200)
            .map(_.party.uid) should have length (100)
        }
      }

      "thrown on duplicate additions" in { implicit env =>
        import env.*
        val daniel = participant1.parties.enable("daniel")
        // daml hub expects the hint to be the prefix. so if this doesn't work anymore,
        // it would break some administrative operations of hub
        daniel.toProtoPrimitive should startWith("daniel::")
        // hub also expects that if you re-use the same hint, it doesn't allocate some other
        // name but actually fails
        assertThrowsAndLogsCommandFailures(
          participant1.parties.enable("daniel"),
          _.shouldBeCantonErrorCode(MappingAlreadyExists),
        )
      }

      "allow adding parties that previously have been disabled" in { implicit env =>
        import env.*
        val name = "alberto"
        // Allocate the party
        val partyId = participant1.parties.enable(
          name,
          synchronizeParticipants = Seq(participant1),
          waitForSynchronizer = SynchronizerChoice.Only(Seq(daName)),
        )
        // Check that we see it
        participant1.topology.party_to_participant_mappings
          .list(daId, filterParty = partyId.filterString)
          .loneElement
          .context
          .serial shouldBe PositiveInt.one

        // Disable the party
        participant1.parties.disable(partyId)

        // Check that we see the removal
        eventually() {
          participant1.topology.party_to_participant_mappings
            .list(
              daId,
              filterParty = partyId.filterString,
              operation = Some(TopologyChangeOp.Remove),
            )
            .loneElement
            .context
            .serial shouldBe PositiveInt.two
        }
        // Allocate the party again
        participant1.parties.enable(
          name,
          synchronizeParticipants = Seq(participant1),
          waitForSynchronizer = SynchronizerChoice.Only(Seq(daName)),
        )
        eventually() {
          // Check that we see it again, but now with serial=3
          participant1.topology.party_to_participant_mappings
            .list(daId, filterParty = partyId.filterString)
            .loneElement
            .context
            .serial shouldBe PositiveInt.three
        }
      }

    }

    "updating parties" should {
      "support updating party details" in { implicit env =>
        import env.*
        val allocatePartyDetails = participant1.ledger_api.parties.allocate(
          party = "party1",
          annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"),
        )
        val updatedPartyDetails = participant1.ledger_api.parties.update(
          party = allocatePartyDetails.party,
          modifier = partyDetails => {
            partyDetails.copy(annotations =
              partyDetails.annotations
                .updated("k2", "v2a")
                .updated("k4", "v4")
                .removed("k3")
                .removed("k5")
            )
          },
        )
        updatedPartyDetails shouldBe PartyDetails(
          party = allocatePartyDetails.party,
          isLocal = true,
          annotations = Map("k1" -> "v1", "k2" -> "v2a", "k4" -> "v4"),
          identityProviderId = "",
        )
      }

      "behave orderly when updating non-existent party" in { implicit env =>
        import env.*
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.ledger_api.parties
            .update(
              party = PartyId.tryFromProtoPrimitive("unknownparty::suffix"),
              modifier = partyDetails => {
                partyDetails.copy(annotations = partyDetails.annotations.updated("a", "b"))
              },
            ),
          _.shouldBeCantonErrorCode(PartyNotFound),
        )
      }

    }

    "show allocated parties via list" in { implicit env =>
      import env.*
      // add a party to participant proposal (this would fail #7397)
      participant1.topology.party_to_participant_mappings.propose(
        PartyId(UniqueIdentifier.tryCreate("boris", participant1.id.uid.namespace)),
        newParticipants = Seq(participant2.id -> ParticipantPermission.Submission),
      )
      participant1.parties.list(limit = 50) should have length (50)
      participant1.parties.list(filterParty = "myparty", limit = 50) should have length (50)
    }

    "list filtering works correctly" in { implicit env =>
      import env.*
      participant1.parties
        .list(filterParticipant = participant2.id.filterString)
        .map(_.party) should have length (2)

      participant1.parties.list(filterParty = "LordSandwich").map(_.party) should have length (1)

      participant1.parties
        .list(filterParty = "LordSandwich", filterParticipant = "one")
        .map(_.party) shouldBe empty

      participant1.parties.list(synchronizerIds =
        Set(SynchronizerId.tryFromString("nada::nada"))
      ) shouldBe empty

      val parties = participant1.parties
        .list(
          filterParty = "LordSandwich",
          filterParticipant = "participant",
          synchronizerIds = daId,
        )
        .map(_.party)
      parties should have length (1)
      participant2.parties.disable(parties.loneElement)
      participant2.topology.synchronisation.await_idle()
      eventually() {
        participant2.parties.list(filterParty = "LordSandwich") shouldBe empty
      }

      // test prefix filtering without a namespace
      val partiesStartingWith1 = participant1.parties.list(filterParty = "myparty1").map(_.party)
      forAll(partiesStartingWith1)(_.identifier.unwrap should startWith("myparty1"))
      partiesStartingWith1 should have length 1 + 10 + 1 // party1 + party[10-19] + party100

      // test filtering with a uid with an incomplete namespace
      participant1.parties
        .list(filterParty = s"myparty1::${participant1.namespace.filterString.take(6)}")
        .loneElement
        .party shouldBe PartyId.tryCreate("myparty1", participant1.namespace)
    }

    "support party operations using a non-default identity providers" in { implicit env =>
      import env.*
      val lapi_idp = participant1.ledger_api.identity_provider_config
      val lapi_parties = participant1.ledger_api.parties
      val idpId = "idp-id-" + UUID.randomUUID().toString
      val _ = lapi_idp.create(
        identityProviderId = idpId,
        jwksUrl = "https://jwks:900",
        issuer = UUID.randomUUID().toString,
        audience = Option("someAudience"),
      )
      val details = lapi_parties.allocate(
        party = "party",
        identityProviderId = idpId,
      )
      details.identityProviderId shouldBe idpId
      val _ = lapi_parties.list(identityProviderId = idpId).find(_.party == details.party).value
      lapi_parties
        .update(
          party = details.party,
          identityProviderId = idpId,
          modifier = _.copy(annotations = Map("foo" -> "bar")),
        )
        .annotations shouldBe Map("foo" -> "bar")
    }

    "support updating party's idp id" in { implicit env =>
      import env.*
      val lapi_idp = participant1.ledger_api.identity_provider_config
      val lapi_parties = participant1.ledger_api.parties
      val idpId1 = "idp-id1-" + UUID.randomUUID().toString
      val idpId2 = "idp-id2-" + UUID.randomUUID().toString
      val _ = lapi_idp.create(
        identityProviderId = idpId1,
        jwksUrl = "https://jwks:900",
        issuer = UUID.randomUUID().toString,
        audience = Option("someAudience"),
      )
      val _ = lapi_idp.create(
        identityProviderId = idpId2,
        jwksUrl = "https://jwks:900",
        issuer = UUID.randomUUID().toString,
        audience = Option("someAudience"),
      )
      val partyId = lapi_parties
        .allocate(
          party = "party" + UUID.randomUUID().toString,
          identityProviderId = "",
        )
        .party
      lapi_parties.update_idp(
        party = partyId,
        sourceIdentityProviderId = "",
        targetIdentityProviderId = idpId1,
      )
      val _ = {
        val details = lapi_parties.list(identityProviderId = idpId1).find(_.party == partyId).value
        details.identityProviderId shouldBe idpId1
      }
      lapi_parties.update_idp(
        party = partyId,
        sourceIdentityProviderId = idpId1,
        targetIdentityProviderId = idpId2,
      )
      val _ = {
        val details = lapi_parties.list(identityProviderId = idpId2).find(_.party == partyId).value
        details.identityProviderId shouldBe idpId2
      }
      lapi_parties.update_idp(
        party = partyId,
        sourceIdentityProviderId = idpId2,
        targetIdentityProviderId = "",
      )
      val _ = {
        val details = lapi_parties.list(identityProviderId = "").find(_.party == partyId).value
        details.identityProviderId shouldBe ""
      }
      // Cleanup user and idps
      lapi_idp.delete(identityProviderId = idpId1)
      lapi_idp.delete(identityProviderId = idpId2)
    }

  }

}

class PartyManagementIntegrationTestPostgres extends PartyManagementIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
