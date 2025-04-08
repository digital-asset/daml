// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.damltests.java.transientcontracts.TransientContractsTest
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

@nowarn("msg=match may not be exhaustive")
trait TransientContractIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1

  "Transient contracts split across views are properly archived" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    participant2.synchronizers.connect_local(sequencer1, alias = daName)

    val alice = participant1.parties.enable(
      "Alice",
      synchronizeParticipants = Seq(participant2),
    )
    val bob = participant2.parties.enable(
      "Bob",
      synchronizeParticipants = Seq(participant1),
    )

    participants.all.dars.upload(CantonTestsPath)

    val aliceAndBobCreateCmd =
      new TransientContractsTest(
        alice.toProtoPrimitive,
        Some(bob.toProtoPrimitive).toJava,
      ).create.commands.asScala.toSeq
    val aliceAndBobCreateTx =
      participant1.ledger_api.javaapi.commands.submit_flat(Seq(alice), aliceAndBobCreateCmd)
    val Seq(aliceAndBobContract) =
      JavaDecodeUtil.decodeAllCreated(TransientContractsTest.COMPANION)(aliceAndBobCreateTx)

    val transientCmd =
      new TransientContractsTest(alice.toProtoPrimitive, None.toJava).createAnd
        .exerciseEntryPoint(aliceAndBobContract.id)
        .commands
        .asScala
        .toSeq
    // use `submit` here because the flat stream omits transient contracts
    val transientTx = participant1.ledger_api.javaapi.commands.submit(Seq(alice), transientCmd)
    val transientIds =
      JavaDecodeUtil
        .decodeAllCreatedTree(TransientContractsTest.COMPANION)(transientTx)
        .map(_.id.toLf)
    val archivedContracts = aliceAndBobContract.id.toLf +: transientIds

    archivedContracts should have size 4

    eventually() {
      val pcs = participant1.testing.pcs_search(daName)
      forAll(archivedContracts) { cid =>
        assert(pcs.exists(entry => entry._2.contractId == cid && !entry._1))
      }
    }
  }
}

class TransientContractReferenceIntegrationTestPostgres extends TransientContractIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
