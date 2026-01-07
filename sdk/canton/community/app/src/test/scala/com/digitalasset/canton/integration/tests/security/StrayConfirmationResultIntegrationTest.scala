// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.nonempty.NonEmpty
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Availability
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseH2,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.protocol.{MemberRecipient, RecipientsTree}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
}
import monocle.macros.syntax.lens.*

import java.util.concurrent.atomic.AtomicBoolean

class StrayConfirmationResultIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestSuite {

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  var sequencer: ProgrammableSequencer = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransform(
        // Use a sim clock so that we don't have to worry about timeouts
        ConfigTransforms.useStaticTime
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonExamplesPath)

        sequencer = getProgrammableSequencer(sequencer1.name)
      }

  override val defaultParticipant: String = "participant1"

  lazy val testStrayMediatorResultAttack: SecurityTest = SecurityTest(
    property = Availability,
    asset = "participant",
    attack = Attack(
      actor = "a faulty mediator",
      threat = "sends a confirmation result to a non-informee participant",
      mitigation = "the participant discards the result",
    ),
  )

  "A participant survives getting a confirmation result for a request it has never seen" taggedAs testStrayMediatorResultAttack in {
    implicit env =>
      import env.*

      val participant1Id = participant1.id
      val participant2Id = participant2.id

      val replacedSubmissionRequest = new AtomicBoolean(false)

      sequencer.setPolicy_("capture next confirmation result and add a recipient") {
        submissionRequest =>
          if (
            submissionRequest.sender == mediator1.id && submissionRequest.batch.allMembers == Set(
              participant1Id
            )
          ) {
            val alsoSendToP2 = submissionRequest
              .focus(_.batch.envelopes)
              .modify(_.map(_.focus(_.recipients.trees).modify { rts =>
                RecipientsTree(NonEmpty.mk(Set, MemberRecipient(participant2Id)), Seq.empty) +: rts
              }))
            replacedSubmissionRequest.set(true)
            val signedModifiedRequest = signModifiedSubmissionRequest(
              alsoSendToP2,
              mediator1.underlying.value.replicaManager.mediatorRuntime.value.mediator.syncCrypto,
              Some(environment.now),
            )
            SendDecision.Replace(signedModifiedRequest)
          } else SendDecision.Process
      }

      clue("running a cycle contract where participant2 also receives the mediator verdict") {
        createCycleContract(participant1, participant1.adminParty, "cycle-P1")
      }

      sequencer.resetPolicy()
      replacedSubmissionRequest.get shouldBe true

      clue("running a ping to make sure that all participants are still alive") {
        participant2.health.ping(participant1)
      }
  }
}
