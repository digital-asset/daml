// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.admin.api.client.data.{
  GrpcSequencerConnection,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.PartyId

import scala.concurrent.duration.DurationInt

sealed trait BftSynchronizerBootstrapTemplateTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S2M2

  "BFT Synchronizer" when {

    "Basic synchronizer startup" in { implicit env =>
      import env.*

      // STEP 1: connect participants for the synchronizer via "their" sequencers
      clue("participant1 connects to sequencer1, sequencer2 using connect_local_bft") {
        participant1.synchronizers.connect_local_bft(
          Seq(sequencer1, sequencer2),
          synchronizerAlias = daName,
        )
      }
      clue("participant2 connects to sequencer1, sequencer2 using connect_bft") {
        participant2.synchronizers.connect_bft(
          Seq(sequencer2, sequencer1).map(s =>
            GrpcSequencerConnection.fromInternal(
              s.config.publicApi.clientConfig
                .asSequencerConnection(SequencerAlias.tryCreate(s.name), sequencerId = None)
            )
          ),
          synchronizerAlias = daName,
          physicalSynchronizerId = Some(daId),
        )
      }
      clue("participant3 connects to sequencer1") {
        participant3.synchronizers.connect_local(sequencer1, daName)
      }

      // STEP 2: participants can now transact with each other
      participant1.health.ping(participant2.id)

      // STEP 3: allocate a plain-old-party
      val testPartyP1 =
        participant1.parties.enable(
          "test-party-p1",
          synchronizeParticipants = Seq(participant2),
        )
      val testPartyP2 =
        participant2.parties.enable(
          "test-party-p2",
          synchronizeParticipants = Seq(participant1),
        )
      checkPartiesExist(testPartyP1, testPartyP2)
    }

    "modify priority" in { implicit env =>
      import env.*
      participant1.synchronizers.config(daName).map(_.priority) shouldBe Some(0)
      participant1.synchronizers.modify(daName, _.withPriority(10))
      participant1.synchronizers.config(daName).map(_.priority) shouldBe Some(10)
    }

    "modify sequencerTrustThreshold" in { implicit env =>
      import env.*
      participant2.synchronizers
        .config(daName)
        .map(_.sequencerConnections.sequencerTrustThreshold) shouldBe Some(PositiveInt.one)
      participant2.synchronizers.modify(daName, _.tryWithSequencerTrustThreshold(2))
      participant2.synchronizers
        .config(daName)
        .map(_.sequencerConnections.sequencerTrustThreshold) shouldBe Some(PositiveInt.two)

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant2.synchronizers.modify(daName, _.tryWithSequencerTrustThreshold(3)),
        _.errorMessage should include(
          "Sequencer trust threshold 3 cannot be greater than number of sequencer connections 2"
        ),
        _.errorMessage should include("Command execution failed"),
      )
    }

    "modify submissionRequestAmplification" in { implicit env =>
      import env.*

      participant2.synchronizers
        .config(daName)
        .map(_.sequencerConnections.submissionRequestAmplification) shouldBe
        Some(SubmissionRequestAmplification.NoAmplification)

      val amplification = SubmissionRequestAmplification(PositiveInt.two, 0.second)
      participant2.synchronizers.modify(
        daName,
        _.withSubmissionRequestAmplification(
          SubmissionRequestAmplification(PositiveInt.two, 0.second)
        ),
      )

      participant2.synchronizers
        .config(daName)
        .map(_.sequencerConnections.submissionRequestAmplification) shouldBe
        Some(amplification)
    }

  }

  private def checkPartiesExist(
      parties: PartyId*
  )(implicit env: TestConsoleEnvironment) = {
    import env.*

    val knownParties = Set(
      participant1.id.adminParty,
      participant2.id.adminParty,
      participant3.id.adminParty,
    ) ++ parties

    eventually() {
      val p1Parties = participant1.parties.list()
      p1Parties.map(_.party) should contain theSameElementsAs (knownParties)
    }

    eventually() {
      val p2Parties = participant2.parties.list()
      p2Parties.map(_.party) should contain theSameElementsAs (knownParties)
    }
  }
}

class BftSynchronizerBootstrapTemplateTestDefault extends BftSynchronizerBootstrapTemplateTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

//class BftSynchronizerBootstrapTemplateTestPostgres extends BftSynchronizerBootstrapTemplateTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseBftSequencer(loggerFactory))
//}
