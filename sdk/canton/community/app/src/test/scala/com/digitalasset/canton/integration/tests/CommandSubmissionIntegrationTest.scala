// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.damltests.java.divulgence.DivulgeIouByExercise
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.ContractNotFound
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.PartyId

import scala.jdk.CollectionConverters.*

trait CommandSubmissionIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  private var alice: PartyId = _
  private var bob: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withSetup { env =>
      import env.*
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.dars.upload(CantonExamplesPath)
      alice = participant1.parties.enable("alice")
      bob = participant1.parties.enable("bob")
    }

  "readAs" should {
    "be supported in commands.submit" in { implicit env =>
      import env.*

      val tx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new DivulgeIouByExercise(
          alice.toProtoPrimitive,
          alice.toProtoPrimitive,
        ).create.commands.asScala.toSeq,
      )
      val cid = JavaDecodeUtil.decodeAllCreated(DivulgeIouByExercise.COMPANION)(tx).loneElement.id

      def consumeSelfCmd = cid
        .exerciseConsumeSelf(bob.toProtoPrimitive)
        .commands
        .asScala
        .toSeq

      // Fails without readAs
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.javaapi.commands
          .submit(Seq(bob), consumeSelfCmd, readAs = Seq.empty),
        _.errorMessage should include(ContractNotFound.id),
      )

      // Succeeds with readAs
      participant1.ledger_api.javaapi.commands
        .submit(Seq(bob), consumeSelfCmd, readAs = Seq(alice))
    }
  }
}

//class CommandSubmissionIntegrationTestDefault extends CommandSubmissionIntegrationTest {
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
//}

class CommandSubmissionIntegrationTestPostgres extends CommandSubmissionIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
