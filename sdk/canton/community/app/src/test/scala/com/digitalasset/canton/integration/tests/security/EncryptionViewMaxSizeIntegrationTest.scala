// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Availability
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  SendDecision,
  SendPolicyWithoutTraceContext,
}
import com.digitalasset.canton.topology.ParticipantId
import org.slf4j.event.Level

import scala.concurrent.Promise

/** In this test, we will verify that a contract with a large payload does not crash the participant
  * or other nodes. Instead of using the malicious participant to bypass the validation performed in
  * phase 1, we will create a contract with a large payload that still respects the allowed size.
  * Then, we block the confirmation request with the programmable sequencer and reduce the
  * maxRequestSize via a topology transaction. This way, we can test the behavior in phase 3.
  */
sealed trait EncryptionViewMaxSizeIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with SecurityTestSuite {

  private val nodeAvailability: SecurityTest = SecurityTest(property = Availability, "Canton node")

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(CantonExamplesPath)
      }

  "Encryption view should not be decompressed if it exceeds the max request size" taggedAs nodeAvailability
    .setAttack(
      Attack(
        actor = "malicious participant",
        threat = "perform a denial of service attack with a large command payload",
        mitigation = "reject a command with a large contract view payload in phase 3",
      )
    ) in { implicit env =>
    import env.*

    val until = Promise[Unit]()
    // hold the confirmation request until the new maxRequestSize is set and become effective so the transaction would use this new value
    getProgrammableSequencer(sequencer1.name).setPolicy_("hold confirmation request")(
      holdConfirmationRequest(participant1, until)
    )

    val testId = "A" * 25000

    val cycle =
      new M.Cycle(testId, participant1.id.adminParty.toProtoPrimitive).create.commands.loneElement
    participant1.ledger_api.javaapi.commands
      .submit_async(Seq(participant1.id.adminParty), Seq(cycle))

    // change the maxRequestSize to a smaller value to make sure the request exceed the limit in phase 3
    val lowMaxRequestSize = NonNegativeInt.tryCreate(20000)
    sequencer1.topology.synchronizer_parameters
      .propose_update(
        synchronizerId = daId,
        _.update(maxRequestSize = lowMaxRequestSize.unwrap),
      )

    eventually() {
      Seq[LocalInstanceReference](sequencer1, participant1).forall(node =>
        node.topology.synchronizer_parameters
          .get_dynamic_synchronizer_parameters(daId)
          .maxRequestSize
          .value == lowMaxRequestSize.unwrap
      )
    }

    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
      // unlock the confirmation request
      until.trySuccess(()),
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq(
          (
            _.warningMessage should include regex s"Max bytes to decompress is exceeded. The limit is ${lowMaxRequestSize.unwrap} bytes.",
            "fail to decompress the encryption view",
          ),
          (
            _.shouldBeCantonErrorCode(MalformedRejects.Payloads),
            "local reject verdict",
          ),
        )
      ),
    )

    // make sure that the participant is still healthy
    participant1.health.ping(participant1)
  }

  private def holdConfirmationRequest(
      from: ParticipantId,
      until: Promise[Unit],
  ): SendPolicyWithoutTraceContext = submissionRequest =>
    if (submissionRequest.sender == from && submissionRequest.isConfirmationRequest) {
      SendDecision.HoldBack(until.future)
    } else SendDecision.Process

}

class EncryptionViewMaxSizeIntegrationTestPostgres extends EncryptionViewMaxSizeIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
