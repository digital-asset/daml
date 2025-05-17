// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.StorageConfig
import com.digitalasset.canton.damltests.java.noninformeestakeholder.{Inner, Outer}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.*

@nowarn("msg=match may not be exhaustive")
trait NonInformeeStakeholderIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1

  "A participant may witness a non-informee stakeholder action" in { implicit env =>
    import env.*

    participants.all.synchronizers.connect_local(sequencer1, alias = daName)
    participants.all.dars.upload(CantonTestsPath)

    val alice = participant1.parties.enable(
      "Alice",
      synchronizeParticipants = Seq(participant2),
    )
    val bob = participant2.parties.enable(
      "Bob",
      synchronizeParticipants = Seq(participant1),
    )

    val createInner =
      new Inner(alice.toProtoPrimitive, bob.toProtoPrimitive).create.commands.asScala.toSeq

    val createOuter =
      new Outer(alice.toProtoPrimitive, bob.toProtoPrimitive).create.commands.asScala.toSeq

    val createTx =
      participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        createInner ++ createOuter,
      )
    val Seq(inner) =
      JavaDecodeUtil.decodeAllCreated(Inner.COMPANION)(createTx)
    val Seq(outer) = JavaDecodeUtil.decodeAllCreated(Outer.COMPANION)(createTx)

    logger.info("Now sending transaction with a stakeholder witnessing an action")
    val exercise = outer.id.exerciseUseInner(inner.id).commands.asScala.toSeq
    participant1.ledger_api.javaapi.commands
      .submit(Seq(alice), exercise)
  }
}

class NonInformeeStakeholderReferenceIntegrationTestInMemory
    extends NonInformeeStakeholderIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[StorageConfig.Memory](loggerFactory))
}
