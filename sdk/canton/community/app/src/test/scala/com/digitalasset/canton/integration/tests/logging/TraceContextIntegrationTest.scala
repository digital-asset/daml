// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.logging

import cats.syntax.functor.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.IntegrationTestUtilities.grabCounts
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.api.services.CommandSubmissionService
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.admin.PingService
import com.digitalasset.canton.participant.protocol.TransactionProcessor
import com.digitalasset.canton.participant.sync.CantonSyncService
import org.scalatest.compatible.Assertion
import org.slf4j.event.Level

abstract class TraceContextIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {
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

  val CaptureLoggersUsingTraceIdForPing: SuppressionRule =
    SuppressionRule.forLogger[PingService] ||
      SuppressionRule.forLogger[CommandSubmissionService] ||
      SuppressionRule.forLogger[CantonSyncService] ||
      SuppressionRule.forLogger[TransactionProcessor] &&
      SuppressionRule.LevelAndAbove(Level.DEBUG)

  /** Integration test to verify that trace-ids are flowing through the entirety of Canton's
    * internals
    */
  "check trace id set on sync service write is visible on related read events" in { implicit env =>
    import env.*

    loggerFactory.assertLogsSeq(CaptureLoggersUsingTraceIdForPing)(
      {
        val p1InitialCounts = grabCounts(daName, participant1)
        val p2InitialCounts = grabCounts(daName, participant2)

        assertPingSucceeds(participant1, participant2)

        // Need to wait until both p1 and p2 have accepted the ping (on the canton sync service level)
        eventually() {
          val p1Counts = grabCounts(daName, participant1)
          val p2Counts = grabCounts(daName, participant2)
          assertResult(
            2
          )(p1Counts.acceptedTransactionCount - p1InitialCounts.acceptedTransactionCount)
          assertResult(p1Counts.plus(p2InitialCounts))(p2Counts.plus(p1InitialCounts))
        }
      },
      { messages =>
        // group related log messages by trace-id that involve submitting a ledger-api transaction
        val submissionTraces = messages
          .map(entry => (entry.mdc.get("trace-id"), entry))
          .collect { case (Some(traceId), entry) => (traceId, entry) }
          .groupBy(_._1)
          .fmap(_.map(_._2))
          .filter(_._2.exists(_.message contains "Successfully submitted transaction"))

        def isPingServicePingCommand(
            entry: LogEntry
        ): Boolean =
          entry.loggerName.contains(classOf[PingService].getSimpleName) &&
            entry.message.contains("Starting ping")

        def isCommandServicePingCommand(
            entry: LogEntry
        ): Boolean =
          entry.loggerName.contains(classOf[CommandSubmissionService].getSimpleName) &&
            entry.message.contains("Submitted commands are: 'create Canton.Internal.Ping:Ping'")

        def hasTransactionAcceptedFor(participant: String)(logEntry: LogEntry): Assertion = {
          logEntry.loggerName should include(participant)
          logEntry.message should include("TransactionAccepted")
        }

        // Ping submission trace exists
        submissionTraces.exists { case (_traceId, entries) =>
          entries.exists(isPingServicePingCommand) &&
          entries.exists(isCommandServicePingCommand) &&
          entries.exists(_.message contains "Successfully submitted transaction")
        } shouldBe true withClue "full Ping submission trace not found"

        forAll(submissionTraces) {
          // we already know that these traces are submitting a ledger-api transaction
          // check that they included a transaction accepted for each participant (demonstrating a successful trace between nodes)
          case (traceId, entries) =>
            forAtLeast(1, entries)(hasTransactionAcceptedFor("participant1"))
            forAtLeast(1, entries)(hasTransactionAcceptedFor("participant2"))
        }
      },
    )
  }
}

class GrpcTraceContextIntegrationTestPostgres extends TraceContextIntegrationTest {
  // run with postgres to ensure writing to persistent stores is working correctly
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
