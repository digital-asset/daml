// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.logging

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.sync.CantonSyncService
import org.slf4j.event.Level

abstract class TraceIdIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .withSetup { implicit env =>
        import env.*

        participants.local.foreach(_.synchronizers.connect_local(sequencer1, alias = daName))

        // Wait until all participants are connected
        utils.retry_until_true {
          participants.local.forall(_.synchronizers.active(daName))
        }
      }

  private val syncService = classOf[CantonSyncService].getSimpleName
  private val lapiService = "CommandSubmissionServiceImpl"

  val CaptureLoggersUsingTraceId: SuppressionRule =
    (SuppressionRule.LoggerNameContains(syncService) ||
      SuppressionRule.LoggerNameContains(lapiService)) &&
      SuppressionRule.LevelAndAbove(Level.DEBUG)

  /** Integration test to verify that trace-ids are flowing through the ledger api server */
  "check trace id set on sync service write is visible on ledger api server events" in {
    implicit env =>
      import env.*

      loggerFactory.assertLogsSeq(CaptureLoggersUsingTraceId)(
        assertPingSucceeds(participant1, participant2),
        { messages =>
          val tracedLogs: Seq[(String, LogEntry)] = messages
            .map(entry => (entry.mdc.get("trace-id"), entry))
            .collect { case (Some(traceId), entry) => (traceId, entry) }

          val syncServiceLogs: Seq[(String, LogEntry)] = tracedLogs
            .filter(_._2.loggerName contains syncService)
            .filter(_._2.message contains "Successfully submitted transaction")
          val syncServiceTids: Seq[String] = syncServiceLogs
            .map(_._1)

          val lapiLogs: Seq[(String, LogEntry)] = tracedLogs
            .filter(_._2.loggerName contains lapiService)
            .filter(_._2.message contains "Submitting commands for interpretation")
          val lapiTids: Seq[String] = lapiLogs
            .map(_._1)

          assert(
            syncServiceTids.nonEmpty,
            "No submissions with trace-ids were found in sync service logs",
          )

          assert(lapiLogs.nonEmpty, "No logs from ledger api server were found")
          assert(lapiTids.nonEmpty, "No trace-ids from ledger api server were found")

          assertResult(
            syncServiceTids,
            s"Trace-ids between sync service and ledger api server differ, sync service submission messages: $syncServiceLogs," +
              s"ledger api server messages: $lapiLogs",
          )(lapiTids)
        },
      )
  }
}

class TraceIdIntegrationTestDefault extends TraceIdIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}
