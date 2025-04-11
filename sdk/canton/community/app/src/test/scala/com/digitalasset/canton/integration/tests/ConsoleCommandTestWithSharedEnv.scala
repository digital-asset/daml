// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.javaapi.data.Command
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.PartyId

import java.time.Instant
import scala.jdk.CollectionConverters.*

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
sealed trait ConsoleCommandIntegrationTestWithSharedEnv
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with EntitySyntax {

  private var bank: PartyId = _
  private var alice: PartyId = _
  private var bob: PartyId = _

  private val amount: Amount = new Amount(100.toBigDecimal, "USD")

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .withSetup { env =>
        import env.*

        participants.all.foreach(_.synchronizers.connect_local(sequencer1, alias = daName))
        bank = participant1.parties.enable("Bank", synchronizeParticipants = Seq(participant2))
        alice = participant1.parties.enable("Alice", synchronizeParticipants = Seq(participant1))
        bob = participant2.parties.enable("Bob", synchronizeParticipants = Seq(participant1))

        participant1.dars.upload(CantonExamplesPath)
        participant2.dars.upload(CantonExamplesPath)
        val commands = Seq.fill[Command](3)(createIouWithObserver(bob))
        submitAndReturnIous(participant1, bank, commands).discard
      }

  override val defaultParticipant: String = "participant1"

  "the command testing.bong" should {
    "work with additional validators" in { implicit env =>
      import env.*

      participant1.testing.maybe_bong(
        targets = Set(participant1, participant2),
        validators = Set(participant2),
        levels = 1,
      ) shouldBe defined
    }
  }

  "setting a limit" should {
    "work with state.acs.of_all()" in { implicit env =>
      import env.*

      val contractsLimited = participant1.ledger_api.state.acs.of_all(limit = 2)
      val contractsAll = participant1.ledger_api.state.acs.of_all(limit = 100)

      contractsLimited.size shouldBe 2
      // 3 more Ious are created
      contractsAll.size shouldBe 3
    }

    "work with state.acs.of_party()" in { implicit env =>
      import env.*

      val contractsLimited = participant1.ledger_api.state.acs.of_party(bank, limit = 2)
      contractsLimited.size shouldBe 2
    }

    "work with sequencer_messages()" in { implicit env =>
      import env.*

      val messages = participant1.testing.sequencer_messages(daName, limit = 3)
      messages.size shouldBe 3
    }

    "work with packages.list()" in { implicit env =>
      import env.*

      val packages = participant1.packages.list(limit = 3)
      packages.size shouldBe 3
    }

    "work with dars.list()" in { implicit env =>
      import env.*

      // 2 DARs are uploaded -> set limit 1 to for testing
      val dars = participant1.dars.list(limit = 1)
      dars.size shouldBe 1
    }

    "work with ledger_api_v2.packages.list()" in { implicit env =>
      import env.*

      val packages = participant1.ledger_api.packages.list(limit = 3)
      packages.size shouldBe 3
    }
  }

  "setting a time range" should {
    val bigBang = Instant.ofEpochMilli(0)
    val veryEarly = Instant.ofEpochMilli(1000)
    val endOfTheUniverse = CantonTimestamp.MaxValue.toInstant

    "work with sequencer.messages()" in { implicit env =>
      import env.*

      withClue("In the first second of the universe, no messages should be on the sequencer") {
        participant1.testing
          .sequencer_messages(daName, Some(bigBang), Some(veryEarly))
          .size shouldBe 0
      }

      withClue("Limit should work in conjunction with from:to filtering") {
        participant1.testing
          .sequencer_messages(daName, Some(bigBang), Some(endOfTheUniverse), limit = 5)
          .size shouldBe 5
      }

      val allMessages = participant1.testing.sequencer_messages(daName).toIndexedSeq
      assert(allMessages.size >= 3, "test assumes that sequencer contains at least 3 messages")
      val timeSecondSequencerMsg = allMessages(1).timestamp.toInstant
      val timeLastSequencerMsg = allMessages.lastOption.value.timestamp.toInstant

      withClue("Should contain first two sequencer messages") {
        participant1.testing
          .sequencer_messages(daName, Some(bigBang), Some(timeSecondSequencerMsg))
          .size shouldBe 2

        participant1.testing
          .sequencer_messages(daName, to = Some(timeSecondSequencerMsg))
          .size shouldBe 2
      }

      withClue("Should contain all but first sequencer messages") {
        participant1.testing
          .sequencer_messages(daName, Some(timeSecondSequencerMsg), Some(timeLastSequencerMsg))
          .size shouldBe allMessages.size - 1

        // note: no test with only 'from' here because new messages may have arrived by now
      }

    }
  }

  "error tracing" should {
    "report the invocation site of the command" in { implicit env =>
      import env.*

      val name = "FailSecondTime"
      participant1.parties.enable(name)
      assertThrowsAndLogsCommandFailures(
        participant1.parties.enable(name),
        _.errorMessage should include("enable invoked from ConsoleCommandTest"),
      )
    }

  }

  private def createIouWithObserver(observer: PartyId): Command =
    new Iou(
      bank.toProtoPrimitive,
      alice.toProtoPrimitive,
      amount,
      List(alice.toProtoPrimitive, observer.toProtoPrimitive).asJava,
    ).create.commands.loneElement

  private def submitAndReturnIous(
      participantRef: ParticipantReference,
      submitter: PartyId,
      commands: Seq[Command],
  ): Seq[Iou.Contract] =
    JavaDecodeUtil
      .decodeAllCreatedTree(Iou.COMPANION)(
        participantRef.ledger_api.javaapi.commands.submit(Seq(submitter), commands)
      )
}

//class ConsoleCommandReferenceIntegrationTestWithSharedEnvDefault extends ConsoleCommandIntegrationTestWithSharedEnv {
//  registerPlugin(new UseReferenceBlockSequencer[H2](loggerFactory))
//}

final class ConsoleCommandReferenceIntegrationTestWithSharedEnvPostgres
    extends ConsoleCommandIntegrationTestWithSharedEnv {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[Postgres](loggerFactory))
}
