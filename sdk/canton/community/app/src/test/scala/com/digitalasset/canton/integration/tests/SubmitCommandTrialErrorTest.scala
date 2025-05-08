// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.javaapi.data.Command
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.{
  UnknownInformees,
  UnknownSubmitters,
}
import com.digitalasset.canton.examples.java.iou.*
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.PackageSelectionFailed
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.NotFound
import com.digitalasset.canton.topology.PartyId

import scala.jdk.CollectionConverters.*

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
trait SubmitCommandTrialErrorTest extends CommunityIntegrationTest with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*
      participants.all.synchronizers.connect_local(sequencer1, daName)
    }

  val amount: Amount = new Amount(100.toBigDecimal, "CHF")

  var Bank: PartyId = _
  var Alice: PartyId = _
  var cmds: Seq[Command] = _

  "Canton" can {
    "not execute a command due to a missing package on the submitter" in { implicit env =>
      import env.*

      val submitterForUnknownPackageTest =
        participant1.parties.enable("SubmitterForUnknownPackageTest")
      val observerForUnknownPackageTest =
        participant1.parties.enable("ObserverForUnknownPackageTest")

      cmds = new Iou(
        submitterForUnknownPackageTest.toProtoPrimitive,
        observerForUnknownPackageTest.toProtoPrimitive,
        amount,
        List.empty.asJava,
      ).create.commands.asScala.toSeq

      assertThrowsAndLogsCommandFailures(
        participant1.ledger_api.javaapi.commands.submit(Seq(submitterForUnknownPackageTest), cmds),
        x => {
          x.shouldBeCommandFailure(NotFound.Package)
          x.message should include("Iou:Iou")
        },
      )

    }

    "not execute a command due to unknown parties" in { implicit env =>
      import env.*

      participant1.dars.upload(CantonExamplesPath)

      assertThrowsAndLogsCommandFailures(
        participant1.ledger_api.javaapi.commands
          .submit(Seq(PartyId.tryFromProtoPrimitive("UnknownSubmitter::Foo")), cmds),
        _.commandFailureMessage should include(UnknownSubmitters.id),
      )

      Bank = participant1.parties.enable("Bank")

      cmds = new Iou(
        Bank.toProtoPrimitive,
        "Alice::foo",
        amount,
        List.empty.asJava,
      ).create.commands.asScala.toSeq

      assertThrowsAndLogsCommandFailures(
        participant1.ledger_api.javaapi.commands.submit(Seq(Bank), cmds),
        _.commandFailureMessage should include(UnknownInformees.id),
      )
    }

    "not execute a command due to missing package on a non-confirmer" in { implicit env =>
      import env.*

      Alice = participant2.parties.enable(
        "Alice",
        synchronizeParticipants = Seq(participant1),
      )
      cmds = new Iou(
        Bank.toProtoPrimitive,
        Alice.toProtoPrimitive,
        amount,
        List.empty.asJava,
      ).create.commands.asScala.toSeq

      assertThrowsAndLogsCommandFailures(
        participant1.ledger_api.javaapi.commands.submit(Seq(Bank), cmds),
        // TODO(#25385): Improve error assertion once the detailed rejection is propagated
        _.commandFailureMessage should (include(PackageSelectionFailed.id) and include(
          "No synchronizers satisfy the draft transaction topology requirements"
        )),
        //        _.commandFailureMessage should (include(
        //          NoSynchronizerForSubmission.id
        //        ) and include regex "Participant PAR::participant2::.* has not vetted"),
      )
    }

    "finally execute a command" in { implicit env =>
      import env.*

      participant2.dars.upload(CantonExamplesPath)

      participant1.ledger_api.javaapi.commands.submit(Seq(Bank), cmds)
    }

    "avoid a ledger fork" in { implicit env =>
      import env.*

      eventuallyForever() {
        participant1.ledger_api.state.acs.of_party(Bank) should have size 1
        participant2.ledger_api.state.acs.of_party(Alice) should have size 1
      }
    }
  }
}

//class SubmitCommandTrialErrorTestDefault extends SubmitCommandTrialErrorTest {
//  registerPlugin(
//    new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory)
//  )
//}

class SubmitCommandTrialErrorTestPostgres extends SubmitCommandTrialErrorTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )
}
