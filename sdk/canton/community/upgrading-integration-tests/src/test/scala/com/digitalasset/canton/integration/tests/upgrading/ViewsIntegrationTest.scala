// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.digitalasset.canton.damltests.upgrade
import com.digitalasset.canton.integration.tests.upgrading.UpgradingBaseTest.CommandsWithExplicitPackageId
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.PartyId

import scala.jdk.CollectionConverters.CollectionHasAsScala

class ViewsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with UpgradingBaseTest.WhenPV {

  private def party(name: String)(implicit env: TestConsoleEnvironment): PartyId =
    env.participant1.parties.list(name).headOption.valueOrFail("where is " + name).party

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1

  "Inter view upgrading" whenUpgradeTestPV {

    "setup the stage" in { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant2.synchronizers.connect_local(sequencer1, alias = daName)

      participant1.dars.upload(UpgradingBaseTest.UpgradeV1)
      participant1.dars.upload(UpgradingBaseTest.UpgradeV2)

      // V1 is not vetted on participant 2
      participant2.dars.upload(UpgradingBaseTest.UpgradeV2)

      participant1.parties.enable(
        "alice",
        synchronizeParticipants = Seq(participant2),
      )
      participant2.parties.enable(
        "bob",
        synchronizeParticipants = Seq(participant1),
      )
    }

    "not require the package used in the creating view" ignore { implicit env =>
      val alice = party("alice")
      val bob = party("bob")

      env.participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new upgrade.v1.java.upgrade.InterView(
          alice.toProtoPrimitive,
          bob.toProtoPrimitive,
        ).createAnd()
          .exerciseDelegateChoice()
          .commands()
          .asScala
          .map(_.withPackageId(upgrade.v1.java.upgrade.InterView.PACKAGE_ID))
          .toList,
      )
    }
  }
}
