// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.api.v2.EventOuterClass
import com.daml.ledger.api.v2.value.Identifier
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.BigDecimalImplicits.DoubleToBigDecimal
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.topology.PartyId
import org.scalatest.OptionValues

import scala.jdk.CollectionConverters.*

trait ConsoleCommandIntegrationTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with HasCycleUtils
    with EntitySyntax
    with OptionValues {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup(env => initializeParties(env))

  override val defaultParticipant: String = "participant1"

  private val testParty = "consoleCommandTestParty"

  private def initializeParties(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    // Setup participant1
    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    utils.retry_until_true(participant1.synchronizers.active(daName))
    participant1.dars.upload(CantonExamplesPath)
    participant1.parties.enable(name = testParty)
  }

  "the command enable" should {

    "fail when a party name with length > 185 is given" in { implicit env =>
      import env.*

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.parties.enable("a" * 186),
        _.errorMessage should include("maximum length of 185"),
      )
    }

    "succeed when party name has length 185" in { implicit env =>
      import env.*
      participant1.parties.enable("a" * 185)
    }
  }

  "the command allocate" should {
    "succeed when party name has length at most 185" in { implicit env =>
      // see commands in `UniqueIdentifier` for why this will fail with party names of length >185
      import env.*
      participant1.ledger_api.parties.allocate("a" * 185)
    }
  }

  "the command modify" should {
    "fail when attempting to configure the SynchronizerAlias" in { implicit env =>
      import env.*
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.synchronizers
          .modify(daName, _.copy(synchronizerAlias = SynchronizerAlias.tryCreate("da2"))),
        _.errorMessage should include(
          "We don't support modifying the synchronizer alias of a SynchronizerConnectionConfig"
        ),
      )

    }
  }

  "the command acs.of_all" should {
    "succeed when never connected to a synchronizer" in { implicit env =>
      import env.*

      val acs = participant1.ledger_api.state.acs.of_all()
      acs.size shouldBe 0
    }

    "succeed when disconnected from a synchronizer" in { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      val acsBefore = participant1.ledger_api.state.acs.of_all()
      createCycleContract(participant1, participant1.adminParty, "cycle contract p1 p2")
      participant1.synchronizers.disconnect(daName)

      val acsAfter = participant1.ledger_api.state.acs.of_all()
      (acsAfter.size - acsBefore.size) shouldBe 1
    }

    "succeed when there are only admin parties" in { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      val acsBefore = participant1.ledger_api.state.acs.of_all()
      createCycleContract(participant1, participant1.adminParty, "cycle contract p1 p2")
      val acsAfter = participant1.ledger_api.state.acs.of_all()

      (acsAfter.size - acsBefore.size) shouldBe 1
    }

    "succeed when parties are allocated" in { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      val alice: String = "Alice"
      participant1.ledger_api.parties.allocate(alice)
      eventually() {
        val plist1 = participant1.parties.list(alice)
        plist1 should have length 1
        plist1.map(_.party.toProtoPrimitive) contains alice
      }

      val acsBefore = participant1.ledger_api.state.acs.of_all()
      createCycleContract(participant1, alice.toPartyId(), "cycle contract p1 alice")
      val acsAfter = participant1.ledger_api.state.acs.of_all()

      (acsAfter.size - acsBefore.size) shouldBe 1
    }
  }

  "the command acs.of_party" should {
    val alice: String = "Alice"

    "succeed when never connected to a synchronizer" in { implicit env =>
      import env.*

      participant1.ledger_api.parties.allocate(alice)

      participant1.ledger_api.state.acs.of_party(alice.toPartyId()) shouldBe empty
      participant1.ledger_api.state.acs.of_party(participant1.adminParty) shouldBe empty
    }

    "succeed when disconnected from a synchronizer" in { implicit env =>
      import env.*

      participant1.ledger_api.parties.allocate(alice)
      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      val acsP1Before = participant1.ledger_api.state.acs.of_party(alice.toPartyId())
      createCycleContract(participant1, alice.toPartyId(), "cycle contract p1 p2")

      participant1.synchronizers.disconnect(daName)

      val acsP1After = participant1.ledger_api.state.acs.of_party(alice.toPartyId())
      (acsP1After.size - acsP1Before.size) shouldBe 1
    }

    "succeed when there are only admin parties" in { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      val acsBefore = participant1.ledger_api.state.acs.of_party(participant1.adminParty)
      createCycleContract(participant1, participant1.adminParty, "cycle contract p1 p2")
      val acsAfter = participant1.ledger_api.state.acs.of_party(participant1.adminParty)

      (acsAfter.size - acsBefore.size) shouldBe 1
    }

    "succeed when parties are allocated" in { implicit env =>
      import env.*

      val aliceId = participant1.parties.enable(alice)

      val acsBefore = participant1.ledger_api.state.acs.of_party(aliceId)

      // Deliberately connecting to the synchronizer after enabling the alice party.
      // This tests whether the party can be enabled without a synchronizer connection and
      // and whether it can be used immediately after connecting.
      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      createCycleContract(participant1, aliceId, "cycle contract p1 alice")
      val acsAfter = participant1.ledger_api.state.acs.of_party(aliceId)

      (acsAfter.size - acsBefore.size) shouldBe 1
    }
  }

  "the service ledger_api.event_query" should {

    def createIou(
        actAs: PartyId,
        participant: LocalParticipantReference,
        payer: PartyId,
        owner: PartyId,
        amount: Amount,
    ): EventOuterClass.CreatedEvent =
      participant.ledger_api.javaapi.commands
        .submit_flat(
          actAs = Seq(actAs),
          commands = new Iou(
            payer.toProtoPrimitive,
            owner.toProtoPrimitive,
            amount,
            List.empty.asJava,
          ).create
            .commands()
            .asScala
            .toSeq,
        )
        .getEvents
        .asScala
        .loneElement
        .toProtoEvent
        .getCreated

    "retrieve events by contract id" in { implicit env =>
      import env.*
      val testPartyId = testParty.toPartyId(participant1)
      val expected = createIou(
        actAs = testPartyId,
        participant = participant1,
        payer = testPartyId,
        owner = testPartyId,
        amount = new Amount(100.0.toBigDecimal, "USD"),
      )
      val actual = participant1.ledger_api.javaapi.event_query
        .by_contract_id(expected.getContractId, Seq(testPartyId))
        .getCreated
        .getCreatedEvent
      actual shouldBe expected
    }

    "template id primitive mapping" in { implicit env =>
      discard(env)
      val expected = Iou.TEMPLATE_ID_WITH_PACKAGE_ID
      val actual = TemplateId.fromJavaIdentifier(expected).toJavaIdentifier
      actual shouldBe expected
    }

    "template id identifier mapping" in { implicit env =>
      discard(env)
      val expected = Identifier("p1", "m1", "e1")
      val actual = TemplateId.fromIdentifier(expected).toIdentifier
      actual shouldBe expected
    }
  }
}

class ConsoleCommandIntegrationTestDefault extends ConsoleCommandIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}

//class ConsoleCommandIntegrationTestPostgres extends ConsoleCommandIntegrationTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
//}
