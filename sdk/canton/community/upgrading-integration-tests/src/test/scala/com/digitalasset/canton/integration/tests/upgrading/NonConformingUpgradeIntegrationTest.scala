// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.commands.{Command, ExerciseCommand}
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.codegen.{Exercised, Update}
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.damltests.nonconforming.v1.java as v1
import com.digitalasset.canton.damltests.nonconforming.v1.java.nonconforming.BankTransfer
import com.digitalasset.canton.damltests.nonconforming.v2.java as v2
import com.digitalasset.canton.integration.ConfigTransforms.disableUpgradeValidation
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.topology.PartyId

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

/** Primary concern is testing run-time model conformance failures
  */
class NonConformingUpgradeIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  registerPlugin(new UseReferenceBlockSequencer[Postgres](loggerFactory))

  private var bank: PartyId = _
  private var alice: PartyId = _
  private var bob: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(disableUpgradeValidation)
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)

        participant1.dars.upload(UpgradingBaseTest.NonConformingV1)
        participant1.dars.upload(UpgradingBaseTest.NonConformingV2)

        bank = participant1.parties.enable("bank")
        alice = participant1.parties.enable("alice")
        bob = participant1.parties.enable("bob")
      }

  /** Upgrading depends on ExerciseActionDescription containing the templateId that can be different
    * from the contract templateId. This change was only done as part of ProtocolVersion.V5 *
    */

  private def getVersion(
      participant: LocalParticipantReference,
      issuer: PartyId,
      exercise: javaapi.data.ExerciseCommand,
      expectException: Boolean = false,
  ): Option[String] =
    if (!expectException) {
      val version = callGetVersion(participant, issuer, exercise)
      Some(version)
    } else {
      intercept[Throwable] {
        callGetVersion(participant, issuer, exercise)
      }
      None
    }

  private def callGetVersion(
      participant: LocalParticipantReference,
      issuer: PartyId,
      exerciseJava: javaapi.data.ExerciseCommand,
  ): String = {
    val exercise = ExerciseCommand.fromJavaProto(exerciseJava.toProto)
    participant.ledger_api.commands
      .submit(
        actAs = Seq(issuer),
        commands = Seq(Command.defaultInstance.withExercise(exercise)),
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      )
      .events
      .loneElement
      .event
      .exercised
      .value
      .exerciseResult
      .value
      .sum
      .text
      .value
  }

  private def checkVersionV1(
      cidV1: v1.nonconforming.NonConforming.ContractId,
      participant: LocalParticipantReference,
      issuer: PartyId,
  ): Unit =
    clue(s"checkVersionV1") {
      getVersion(
        participant,
        issuer,
        cidV1
          .exerciseGetVersion()
          .commands
          .overridePackageId(v1.nonconforming.NonConforming.PACKAGE_ID)
          .loneElement
          .asExerciseCommand
          .toScala
          .value,
      ) shouldBe Some("V1")
    }

  private def checkVersionV2(
      cidV2: v2.nonconforming.NonConforming.ContractId,
      participant: LocalParticipantReference,
      issuer: PartyId,
      expectException: Boolean,
  ): Unit = {
    val expected = if (expectException) None else Some("V2")
    clue(s"checkVersionV2") {
      getVersion(
        participant,
        issuer,
        cidV2
          .exerciseGetVersion()
          .commands
          .overridePackageId(v2.nonconforming.NonConforming.PACKAGE_ID)
          .loneElement
          .asExerciseCommand
          .toScala
          .value,
        expectException,
      ) shouldBe expected
    }
  }

  private def checkVersionCkV1(
      cidV1: v1.nonconforming.NonConformingCK.ContractId,
      participant: LocalParticipantReference,
      issuer: PartyId,
  ): Unit =
    clue(s"checkCkVersionV1") {
      getVersion(
        participant,
        issuer,
        cidV1
          .exerciseGetVersionCK()
          .commands
          .overridePackageId(v1.nonconforming.NonConforming.PACKAGE_ID)
          .loneElement
          .asExerciseCommand
          .toScala
          .value,
      ) shouldBe Some("V1")
    }

  private def checkVersionCkV2(
      cidV2: v2.nonconforming.NonConformingCK.ContractId,
      participant: LocalParticipantReference,
      issuer: PartyId,
      expectException: Boolean,
  ): Unit =
    clue(s"checkCkVersionV2") {
      val expected = if (expectException) None else Some("V2")
      getVersion(
        participant,
        issuer,
        cidV2
          .exerciseGetVersionCK()
          .commands
          .overridePackageId(v2.nonconforming.NonConforming.PACKAGE_ID)
          .loneElement
          .asExerciseCommand
          .toScala
          .value,
        expectException,
      ) shouldBe expected
    }

  private def testNonConformingInMode(mode: Long)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    participant1.ledger_api.javaapi.commands.submit(
      Seq(alice),
      new v1.nonconforming.NonConforming(
        alice.toProtoPrimitive,
        bob.toProtoPrimitive,
        mode,
      ).create.commands.overridePackageId(v1.nonconforming.NonConforming.PACKAGE_ID).asScala.toSeq,
    )

    val cidV1: v1.nonconforming.NonConforming.ContractId =
      participant1.ledger_api.javaapi.state.acs
        .await(v1.nonconforming.NonConforming.COMPANION)(
          alice,
          (c: v1.nonconforming.NonConforming.Contract) =>
            c.data.mode == mode, // Ensure we get the correct creation
        )
        .id
    val cidV2: v2.nonconforming.NonConforming.ContractId =
      new v2.nonconforming.NonConforming.ContractId(cidV1.contractId)

    checkVersionV1(cidV1, participant1, alice)
    checkVersionV2(cidV2, participant1, alice, mode != 0)
  }

  private def testNonConformingCKInMode(mode: Long)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    participant1.ledger_api.javaapi.commands.submit(
      Seq(alice),
      new v1.nonconforming.NonConformingCK(
        alice.toProtoPrimitive,
        bob.toProtoPrimitive,
        mode,
      ).create.commands.overridePackageId(v1.nonconforming.NonConforming.PACKAGE_ID).asScala.toSeq,
    )

    val cidV1: v1.nonconforming.NonConformingCK.ContractId =
      participant1.ledger_api.javaapi.state.acs
        .await(v1.nonconforming.NonConformingCK.COMPANION)(
          alice,
          (c: v1.nonconforming.NonConformingCK.Contract) =>
            c.data.mode == mode, // Ensure correct contract is selected
        )
        .id
    val cidV2: v2.nonconforming.NonConformingCK.ContractId =
      new v2.nonconforming.NonConformingCK.ContractId(cidV1.contractId)

    checkVersionCkV1(cidV1, participant1, alice)
    checkVersionCkV2(cidV2, participant1, alice, expectException = mode != 0)
  }

  "Non Conformance" when {

    "In no-change mode (0) upgrade is allowed" in { implicit env =>
      testNonConformingInMode(0)
    }

    "In signatory change mode (1) upgrade is forbidden" in { implicit env =>
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        testNonConformingInMode(1),
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq(
            (
              _.errorMessage should include regex raw"(?s)INVALID_ARGUMENT/INTERPRETATION_UPGRADE_ERROR_VALIDATION_FAILED.*Interpretation error: Error: Validation fails when trying to upgrade the contract.*from NonConforming:NonConforming.*",
              "mode 1 failure",
            )
          )
        ),
      )
    }

    "In observer change mode (2) upgrade is forbidden" in { implicit env =>
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        testNonConformingInMode(2),
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq(
            (
              _.errorMessage should include regex raw"(?s)INVALID_ARGUMENT/INTERPRETATION_UPGRADE_ERROR_VALIDATION_FAILED.*Interpretation error: Error: Validation fails when trying to upgrade the contract.*from NonConforming:NonConforming.*",
              "mode 2 failure",
            )
          )
        ),
      )
    }
  }

  // Ignore contract key based tests
  if (testedProtocolVersion.isDev) {
    "Non CK Conformance" when {

      "In no-change mode (0) upgrade is allowed" ignore { implicit env =>
        testNonConformingCKInMode(0)
      }

      "In key change mode (1) upgrade is forbidden" ignore { implicit env =>
        loggerFactory.suppressErrors(
          testNonConformingCKInMode(1)
        )
      }

      "In maintainer change mode (2) upgrade is forbidden" ignore { implicit env =>
        loggerFactory.suppressErrors(
          testNonConformingCKInMode(2)
        )
      }

    }
  }

  "Change of template party order" when {

    "party order is switched" in { implicit env =>
      import env.*

      participant1.ledger_api.javaapi.commands.submit(
        Seq(bank),
        new BankTransfer(
          bank.toProtoPrimitive,
          alice.toProtoPrimitive,
          bob.toProtoPrimitive,
          0,
        ).create.commands.overridePackageId(v1.nonconforming.BankTransfer.PACKAGE_ID).asScala.toSeq,
      )

      val cidV1: v1.nonconforming.BankTransfer.ContractId =
        participant1.ledger_api.javaapi.state.acs
          .await(v1.nonconforming.BankTransfer.COMPANION)(bank)
          .id

      def getPayee(update: Update[Exercised[String]], packageIdOverride: String): String =
        participant1.ledger_api.javaapi.commands
          .submit(
            actAs = Seq(bank),
            commands = update
              .commands()
              .overridePackageId(packageIdOverride)
              .asScala
              .toSeq,
            transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
          )
          .getEvents
          .asScala
          .collect { case e: com.daml.ledger.javaapi.data.ExercisedEvent => e }
          .loneElement
          .getExerciseResult
          .asParty()
          .get()
          .getValue

      // V1 works
      getPayee(
        cidV1.exerciseBankTransfer_GetPayee(),
        v1.nonconforming.BankTransfer.PACKAGE_ID,
      )

      val cidV2: v2.nonconforming.BankTransfer.ContractId =
        new v2.nonconforming.BankTransfer.ContractId(cidV1.contractId)

      // V2 fails
      assertThrowsAndLogsCommandFailures(
        getPayee(
          cidV2.exerciseBankTransfer_GetPayee(),
          v2.nonconforming.BankTransfer.PACKAGE_ID,
        ),
        e =>
          e.errorMessage should include regex raw"(?s)INVALID_ARGUMENT/INTERPRETATION_UPGRADE_ERROR_AUTHENTICATION_FAILED.*Error when authenticating contract.*${cidV2.contractId}",
      )

    }
  }

}
