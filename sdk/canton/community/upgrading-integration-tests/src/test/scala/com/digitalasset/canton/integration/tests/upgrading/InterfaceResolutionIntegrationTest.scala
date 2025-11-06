// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.javaapi.data.CreateCommand
import com.daml.ledger.javaapi.data.codegen.{Created, Update}
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.damltests.upgrade.v1.java.upgrade.{
  UpgradeItCallInterface as UpgradeItCallInterfaceV1,
  UpgradeItTemplate as UpgradeItTemplateV1,
}
import com.digitalasset.canton.damltests.upgrade.v1.java.upgradeif.{
  UpgradeItInterface,
  UpgradeItVersionStamp,
}
import com.digitalasset.canton.damltests.upgrade.v2.java.upgrade.UpgradeItTemplate as UpgradeItTemplateV2
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.PartyId

import scala.jdk.CollectionConverters.*

sealed abstract class InterfaceResolutionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        initializedSynchronizers(daName)

        participant1.dars.upload(UpgradingBaseTest.UpgradeV1)
        participant1.dars.upload(UpgradingBaseTest.UpgradeV2)

        participant1.parties.enable("alice")
      }

  private def party(name: String)(implicit env: TestConsoleEnvironment): PartyId =
    env.participant1.parties.list(name).headOption.valueOrFail("where is " + name).party

  "interface resolution" when {

    def setupInterface(implicit env: FixtureParam): (PartyId, UpgradeItInterface.ContractId) = {

      import env.participant1

      val alice = party("alice")

      val templateTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new Update.CreateUpdate[UpgradeItTemplateV1.ContractId, Created[
          UpgradeItTemplateV1.ContractId
        ]](
          new CreateCommand(
            UpgradeItTemplateV1.TEMPLATE_ID_WITH_PACKAGE_ID,
            new UpgradeItTemplateV1(alice.toProtoPrimitive).toValue,
          ),
          identity,
          new UpgradeItTemplateV1.ContractId(_),
        ).commands.asScala.toSeq,
      )
      val templateCid: UpgradeItTemplateV1.ContractId =
        JavaDecodeUtil.decodeAllCreated(UpgradeItTemplateV1.COMPANION)(templateTx).loneElement.id

      val interfaceCid1 = templateCid.toInterface(UpgradeItInterface.INTERFACE)

      (alice, interfaceCid1)
    }

    def testDirect(preferred: LfPackageId, expected: Int)(implicit env: FixtureParam): Unit = {

      import env.*

      val (alice, interfaceCid1) = setupInterface

      val directTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        interfaceCid1.exerciseUpgradeItStamp("direct").commands().asScala.toSeq,
        userPackageSelectionPreference = Seq(preferred),
      )
      JavaDecodeUtil
        .decodeAllCreated(UpgradeItVersionStamp.COMPANION)(directTx)
        .loneElement
        .data
        .stampedVersion shouldBe expected
    }

    def testIndirect(preferred: LfPackageId, expected: Int)(implicit env: FixtureParam): Unit = {

      import env.*

      val (alice, interfaceCid1) = setupInterface

      val callTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new Update.CreateUpdate[UpgradeItCallInterfaceV1.ContractId, Created[
          UpgradeItCallInterfaceV1.ContractId
        ]](
          new CreateCommand(
            UpgradeItCallInterfaceV1.TEMPLATE_ID_WITH_PACKAGE_ID,
            new UpgradeItCallInterfaceV1(alice.toProtoPrimitive).toValue,
          ),
          identity,
          new UpgradeItCallInterfaceV1.ContractId(_),
        ).commands.asScala.toSeq,
      )
      val callCid: UpgradeItCallInterfaceV1.ContractId =
        JavaDecodeUtil.decodeAllCreated(UpgradeItCallInterfaceV1.COMPANION)(callTx).loneElement.id

      val indirectTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        callCid.exerciseUpgradeItCallStamp(interfaceCid1).commands().asScala.toSeq,
        userPackageSelectionPreference = Seq(preferred),
      )
      JavaDecodeUtil
        .decodeAllCreated(UpgradeItVersionStamp.COMPANION)(indirectTx)
        .loneElement
        .data
        .stampedVersion shouldBe expected
    }

    "support interface fetch" in { implicit env =>
      import env.*

      val (alice, interfaceCid1) = setupInterface

      val callTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new UpgradeItCallInterfaceV1(alice.toProtoPrimitive).create.commands.asScala.toSeq,
        userPackageSelectionPreference =
          Seq(LfPackageId.assertFromString(UpgradeItCallInterfaceV1.PACKAGE_ID)),
      )

      val callCid: UpgradeItCallInterfaceV1.ContractId =
        JavaDecodeUtil.decodeAllCreated(UpgradeItCallInterfaceV1.COMPANION)(callTx).loneElement.id

      participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        callCid.exerciseUpgradeItIfFetch(interfaceCid1).commands().asScala.toSeq,
        userPackageSelectionPreference = Seq(
          LfPackageId.assertFromString(UpgradeItTemplateV1.TEMPLATE_ID_WITH_PACKAGE_ID.getPackageId)
        ),
      )

    }

    "direct dispatch to v1" in { implicit env =>
      testDirect(LfPackageId.assertFromString(UpgradeItTemplateV1.PACKAGE_ID), 1)
    }

    "direct dispatch to v2" in { implicit env =>
      testDirect(LfPackageId.assertFromString(UpgradeItTemplateV2.PACKAGE_ID), 2)
    }

    "indirect dispatch to v1" in { implicit env =>
      testIndirect(LfPackageId.assertFromString(UpgradeItTemplateV1.PACKAGE_ID), 1)
    }

    "indirect dispatch to v2" in { implicit env =>
      testIndirect(LfPackageId.assertFromString(UpgradeItTemplateV2.PACKAGE_ID), 2)
    }

  }

}

final class InterfaceResolutionIntegrationRefTest extends InterfaceResolutionIntegrationTest {
  registerPlugin(new UseReferenceBlockSequencer[Postgres](loggerFactory))
}
