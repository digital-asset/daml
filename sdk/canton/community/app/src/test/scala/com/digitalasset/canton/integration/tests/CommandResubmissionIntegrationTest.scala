// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.isConfirmationResponse
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.topology.*

import java.util.concurrent.atomic.AtomicBoolean

trait CommandResubmissionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private val threeSeconds = config.NonNegativeFiniteDuration.ofSeconds(3)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(ConfigTransforms.useStaticTime)
      .withSetup { env =>
        import env.*
        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(
            confirmationResponseTimeout = threeSeconds,
            mediatorReactionTimeout = threeSeconds,
          ),
        )
      }

  "A ping succeeds even if the first attempt times out" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, alias = daName)

    val sequencer = getProgrammableSequencer(sequencer1.name)

    val delayDone = new AtomicBoolean(false)

    sequencer.setPolicy_("delay the first confirmation response sent by a participant") { request =>
      request.sender match {
        case _: ParticipantId if !delayDone.get && isConfirmationResponse(request) =>
          environment.simClock.value.advance(threeSeconds.plusSeconds(1).asJava)
          delayDone.set(true)
        case _ =>
        // No action needs to be taken in any other case
      }
      SendDecision.Process
    }
    loggerFactory.assertLogsUnordered(
      assertPingSucceeds(
        participant1,
        participant1,
        timeoutMillis = 15000,
        id = "pingNeedingRetry",
      ),
      _.warningMessage should (include("Response message for request") and include(
        "timed out at"
      )),
    )

    // make sure that at least one command submission was rejected
    participant1.ledger_api.completions.list(
      partyId = participant1.adminParty, // ping party
      atLeastNumCompletions = 1, // waiting for the first reject
      beginOffsetExclusive = 0L,
      userId = "PingService", // ping user id
      filter = completion =>
        completion.updateId.isEmpty && // meaning: rejection
          completion.synchronizerTime.value.synchronizerId == daId.toProtoPrimitive, // for the da synchronizer
    ) should not be empty
  }

}

class CommandResubmissionReferenceIntegrationTestPostgres
    extends CommandResubmissionIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
