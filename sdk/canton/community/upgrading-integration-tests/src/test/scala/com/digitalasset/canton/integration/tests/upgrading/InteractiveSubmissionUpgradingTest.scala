// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.commands.{Command, DisclosedContract}
import com.daml.ledger.api.v2.event.CreatedEvent.toJavaProto
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.damltests.upgrade.v1.java.upgrade.Upgrading as UpgradingV1
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.tests.ledgerapi.submission.InteractiveSubmissionIntegrationTestSetup

class InteractiveSubmissionUpgradingTest extends InteractiveSubmissionIntegrationTestSetup {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*
        participant1.dars.upload(UpgradingBaseTest.UpgradeV1)
        participant1.dars.upload(UpgradingBaseTest.UpgradeV2)
        participant2.dars.upload(UpgradingBaseTest.UpgradeV2)
      }

  "Interactive submission" should {

    // TODO(#23876) - remove ignore
    "prepare a transaction without the creating package of an input contract" ignore {
      implicit env =>
        import env.*

        val bobE = participant1.parties.external.enable("BobE")

        // Create a V1 contract with alice on P1
        val commands = new UpgradingV1(
          bobE.toProtoPrimitive,
          bobE.toProtoPrimitive,
          100,
        )
        val prepared = participant1.ledger_api.interactive_submission.prepare(
          Seq(bobE.partyId),
          Seq(Command.fromJavaProto(commands.create.commands.loneElement.toProtoCommand)),
          userPackageSelectionPreference =
            Seq(LfPackageId.assertFromString(UpgradingV1.PACKAGE_ID)), // Force V1
        )
        val execResponse = execAndWait(
          prepared,
          Map(bobE.partyId -> global_secret.sign(prepared.preparedTransactionHash, bobE)),
        )
        val event = findTransactionByUpdateId(
          bobE,
          execResponse.updateId,
          verbose = true,
        ).events.head.getCreated

        val contract = UpgradingV1.Contract.fromCreatedEvent(
          com.daml.ledger.javaapi.data.CreatedEvent.fromProto(toJavaProto(event))
        )
        val changeOwnerCommands =
          contract.id.exerciseChangeOwner(bobE.toProtoPrimitive).commands()

        // Exercise a choice on the contract with explicit disclosure, on P2, which only has V2 of the package
        val disclosedContract = DisclosedContract(
          event.templateId,
          event.contractId,
          event.createdEventBlob,
          daId.logical.toProtoPrimitive,
        )

        participant2.ledger_api.javaapi.commands.submit(
          Seq(bobE),
          Seq(changeOwnerCommands.loneElement),
          disclosedContracts = Seq(
            com.daml.ledger.javaapi.data.DisclosedContract
              .fromProto(DisclosedContract.toJavaProto(disclosedContract))
          ),
        )
    }
  }
}
