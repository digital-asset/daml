// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.TopologyTimestampAfterSequencingTimestamp
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.util.ShowUtil.*
import monocle.macros.syntax.lens.*

import scala.jdk.CollectionConverters.*

trait DeliverErrorIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with HasProgrammableSequencer {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      // Use a sim clock so that we don't have to worry about reaction timeouts
      .addConfigTransform(ConfigTransforms.useStaticTime)

  override val defaultParticipant: String = "participant1"

  "participants can handle a DeliverError" in { implicit env =>
    import env.*

    // If one of the recipients of a batch is not known to the sequencer,
    // the sequencer will return a deliver error

    List(participant1, participant2).foreach { p =>
      p.synchronizers.connect_local(sequencer1, daName)
      p.dars.upload(CantonExamplesPath)
    }

    val participant1Id = participant1.id
    val sequencer = getProgrammableSequencer(sequencer1.name)

    val alice = participant1.parties.enable("Alice", synchronizeParticipants = Seq(participant2))
    val bob = participant2.parties.enable("Bob", synchronizeParticipants = Seq(participant1))

    val syncCrypto = participant1.underlying.value.sync.syncCrypto

    sequencer.setPolicy_("set a bad topology timestamp") { submissionRequest =>
      submissionRequest.sender match {
        case `participant1Id` if submissionRequest.isConfirmationRequest =>
          val modifiedRequest = submissionRequest
            .focus(_.topologyTimestamp)
            .replace(Some(CantonTimestamp.MaxValue))
          // We now must recreate a correct signature of the sender
          val signedModifiedRequest =
            signModifiedSubmissionRequest(
              modifiedRequest,
              syncCrypto.tryForSynchronizer(daId, defaultStaticSynchronizerParameters),
            )
          SendDecision.Replace(signedModifiedRequest)

        case _ => SendDecision.Process
      }
    }

    val iouCommand = new Iou(
      alice.toProtoPrimitive,
      bob.toProtoPrimitive,
      new Amount(1.toBigDecimal, "snack"),
      List.empty.asJava,
    ).create.commands.asScala.toSeq
    val expectedErrorCode = TopologyTimestampAfterSequencingTimestamp.id

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.ledger_api.javaapi.commands.submit(Seq(alice), iouCommand),
      _.warningMessage should include("Submission was rejected by the sequencer at"),
      _.errorMessage should (include("Request failed for participant1") and include(
        show"The topology timestamp must be before or at"
      ) and include(expectedErrorCode)),
    )

    logger.info("received a deliver error for the submission")

    // Make sure that both participants are still alive
    sequencer.resetPolicy()
    participant1.health.ping(participant2)
  }
}

class DeliverErrorReferenceIntegrationTestPostgres extends DeliverErrorIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

class DeliverErrorBftOrderingIntegrationTestPostgres extends DeliverErrorIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
