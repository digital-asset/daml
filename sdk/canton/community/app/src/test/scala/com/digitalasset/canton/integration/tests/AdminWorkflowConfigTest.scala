// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal.ping.Ping
import com.digitalasset.canton.participant.admin.{AdminWorkflowServices, PingService}
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import org.slf4j.event.Level

import scala.concurrent.duration.*

trait AdminWorkflowConfigTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {
  lazy val timeoutMillis: Long = 1000L

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, daName)
      }

  "ping timeouts" in { implicit env =>
    import env.*

    val sequencer = getProgrammableSequencer(sequencer1.name)
    val participant1id = participant1.id // Do not inline as this is a gRPC call
    sequencer.setPolicy_("delay all messages from participant1") { submissionRequest =>
      if (submissionRequest.sender == participant1id) SendDecision.Delay(timeoutMillis.millis)
      else SendDecision.Process
    }
    loggerFactory.assertEventuallyLogsSeq(
      SuppressionRule.forLogger[PingService] && SuppressionRule.Level(Level.WARN)
    )(
      {
        // The ping service timeout also determines how long the whole ping may take.
        val commandId1 = "AdminWorkflowConfigTest-first-ping"
        participant1.health.maybe_ping(
          participant1,
          timeout = timeoutMillis.millis,
          id = commandId1,
        ) shouldBe None

        // The ping service will wait for a long time,
        // thus finish the whole ping.
        val commandId2 = "AdminWorkflowConfigTest-second-ping"
        participant1.health.ping(
          participant1,
          timeout = (60 * 1000L).millis,
          id = commandId2,
        )

        // Wait for both Ping to finish in the background
        eventually() {
          participant1.ledger_api.state.acs
            .of_all(
              limit = PositiveInt.tryCreate(4),
              filterTemplates = TemplateId.templateIdsFromJava(Ping.TEMPLATE_ID),
            )
            .size shouldBe 0
        }
      },
      allPingServiceLogs => {
        allPingServiceLogs shouldBe empty
      },
    )

    // Reset the policy to avoid shutdown issues
    sequencer.resetPolicy()
  }

  val ReUploadSuppressionRule: SuppressionRule =
    SuppressionRule.forLogger[AdminWorkflowServices] && SuppressionRule.Level(Level.DEBUG)

  "Dar is not re-uploaded if participant is restarted" in { implicit env =>
    import env.*

    def logEntryOfInterest(logEntry: LogEntry): Boolean =
      logEntry.level == Level.DEBUG && logEntry.message
        .contains("Admin workflow packages are already present. Skipping loading.")
    def logEntriesOfInterest(logEntries: Seq[LogEntry]): Seq[LogEntry] =
      logEntries.filter(logEntryOfInterest)

    loggerFactory.assertLogsSeq(ReUploadSuppressionRule)(
      {
        // AdminWorkflowPackages are already loaded from previous ping timeout test
        participant1.stop()
        participant1.start()
      },
      allLogEntries => {
        val logEntries = logEntriesOfInterest(allLogEntries)
        logEntries should not be empty
      },
    )
  }
}

//class AdminWorkflowConfigTestDefault extends AdminWorkflowConfigTest

class AdminWorkflowConfigTestPostgres extends AdminWorkflowConfigTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
