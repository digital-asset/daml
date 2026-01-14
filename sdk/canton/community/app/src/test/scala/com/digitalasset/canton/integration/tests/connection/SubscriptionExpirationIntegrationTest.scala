// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.connection

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.sequencing.{GrpcInternalSequencerConnectionX, SequencerSubscriptionX}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level.INFO

sealed trait SubscriptionExpirationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.setConnectionPool(true),
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.publicApi.maxTokenExpirationInterval)
            .replace(config.NonNegativeFiniteDuration.ofSeconds(2))
        ),
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.sequencerClient.authToken.refreshAuthTokenBeforeExpiry)
            .replace(config.NonNegativeFiniteDuration.ofSeconds(1))
        ),
        ConfigTransforms.updateAllMediatorConfigs_(
          _.focus(_.sequencerClient.authToken.refreshAuthTokenBeforeExpiry)
            .replace(config.NonNegativeFiniteDuration.ofSeconds(1))
        ),
      )

  "Subscriptions closing due to token expiration" must {
    "not trigger connection restarts" in { implicit env =>
      import env.*

      clue("connect") {
        participant1.synchronizers.connect_local(sequencer1, daName)
      }

      def isConnectionRestart(logEntry: LogEntry): Boolean = logEntry.loggerName.contains(
        "GrpcInternalSequencerConnectionX"
      ) && (logEntry.message.startsWith("Starting") || logEntry.message.startsWith("Stopping"))

      def isSubscriptionStart(logEntry: LogEntry): Boolean =
        logEntry.loggerName.contains("SequencerSubscriptionX") && logEntry.message.startsWith(
          "Starting subscription at"
        )

      // With the adjusted timings, new tokens are obtained every second, and subscriptions expire after 2 seconds.
      // By waiting a little bit, we should witness some subscriptions closing and restarting.
      clue("wait for connection or subscription restart") {
        loggerFactory.assertLogsSeq(
          SuppressionRule.LevelAndAbove(INFO) &&
            (SuppressionRule.forLogger[GrpcInternalSequencerConnectionX] ||
              SuppressionRule.forLogger[SequencerSubscriptionX[?]])
        )(
          eventually() {
            Threading.sleep(500)

            // There should be at least 2 subscription restarts (for the participant and the mediator)
            loggerFactory.fetchRecordedLogEntries.count(logEntry =>
              isConnectionRestart(logEntry) || isSubscriptionStart(logEntry)
            ) should be >= 2
          },
          logEntries => {
            forAll(logEntries) { logEntry =>
              // There should be no connection restart.
              // If there is a connection restart, it must precede the subscription start (otherwise we would not have
              // obtained a connection for the subscription), so we would have captured it.
              if (logEntry.loggerName.contains("GrpcInternalSequencerConnectionX"))
                logEntry.message should (not startWith "Starting" and not startWith "Stopping")
              else succeed
            }
          },
        )
      }

      clue("ping") {
        participant1.health.ping(participant1)
      }
    }
  }
}

class SubscriptionExpirationIntegrationTestDefault extends SubscriptionExpirationIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
