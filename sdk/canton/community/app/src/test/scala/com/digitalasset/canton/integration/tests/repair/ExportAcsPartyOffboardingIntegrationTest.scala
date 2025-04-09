// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.File
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.AcsInspection
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.PartyId

/*
In the case of a party migration, the ACS snapshot is used in the last step
to purge the contracts on the source participant. We want to be careful not
to purge contracts that are still needed.

Consider the following scenario:
- P1: hosting Alice, Bank
- Alice and Bank are stakeholders on a Iou
- P2: fresh

In that case, when migrating the Bank from P1 to P2, we should not remove the Iou
from the ACS. Since we don't have the tooling to do that and since it would open
other cans of worms (contract key journal), we make sure that the export_acs_old endpoint
refuses to serve such a request.
 */
class ExportAcsPartyOffboardingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )

  private var alice: PartyId = _
  private var bank: PartyId = _
  private var charlie: PartyId = _

  "topology setup" in { implicit env =>
    import env.*

    participants.all.synchronizers.connect_local(sequencer1, daName)
    participants.all.dars.upload(CantonTestsPath)

    alice = participant1.parties.enable(
      "Alice",
      synchronizeParticipants = Seq(participant2),
    )
    bank = participant1.parties.enable(
      "Bank",
      synchronizeParticipants = Seq(participant2),
    )
    charlie = participant2.parties.enable(
      "Charlie",
      synchronizeParticipants = Seq(participant1),
    )
  }

  "create test contract" in { implicit env =>
    import env.*

    IouSyntax.createIou(participant1)(bank, alice)
  }

  "export_acs_old" should {
    "not allow to download snapshot (offboarding=true) if the snapshot contains contracts that are needed" in {
      implicit env =>
        import env.*

        File.usingTemporaryFile() { file =>
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant1.repair.export_acs_old(
              parties = Set(alice),
              partiesOffboarding = true,
              outputFile = file.canonicalPath,
            ),
            _.commandFailureMessage should (include(
              s"Parties offboarding on synchronizer $daId"
            ) and
              include(
                s"Cannot take snapshot to offboard parties ${List(alice.toProtoPrimitive)}"
              ) and include(
                s"because the following parties have contracts: ${bank.toProtoPrimitive}"
              )),
          )
        }
    }

    "allow to download snapshot (offboarding=true) if the snapshot does not contracts that are needed" in {
      implicit env =>
        import env.*

        File.usingTemporaryFile() { file =>
          participant1.repair.export_acs_old(
            parties = Set(alice, bank),
            partiesOffboarding = true,
            outputFile = file.canonicalPath,
          )
        }
    }

    "allow to download snapshot (offboarding=false) if the snapshot contains contracts that are needed" in {
      implicit env =>
        import env.*

        File.usingTemporaryFile() { file =>
          participant1.repair.export_acs_old(
            parties = Set(alice),
            partiesOffboarding = false,
            outputFile = file.canonicalPath,
          )
        }
    }

    "only consider local parties" in { implicit env =>
      import env.*

      IouSyntax.createIou(participant1)(alice, charlie)
      participant1.topology.party_to_participant_mappings
        .list(daId, filterParticipant = participant1.id.filterString)
        .map(_.item.partyId)
        .toSet shouldBe Set(alice, bank)

      participant2.topology.party_to_participant_mappings
        .list(daId, filterParticipant = participant2.id.filterString)
        .map(_.item.partyId)
        .toSet shouldBe Set(charlie)

      // charlie hosted on P2 does not prevent alice and bob to be offboarded
      File.usingTemporaryFile() { file =>
        participant1.repair.export_acs_old(
          parties = Set(alice, bank),
          partiesOffboarding = true,
          outputFile = file.canonicalPath,
        )
      }
    }
  }
}
