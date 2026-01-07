// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import com.digitalasset.canton.config.PositiveDurationSeconds
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.toxiproxy.ParticipantToSequencerPublicApi
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers.{
  bongWithKillConnection,
  checkLogs,
}
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyParticipantSynchronizerHelpers.connectParticipantsToDa
import com.digitalasset.canton.integration.tests.toxiproxy.{
  ToxiproxyHelpers,
  ToxiproxyParticipantSynchronizerBase,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  IsolatedEnvironments,
}
import com.digitalasset.canton.logging.SuppressionRule
import eu.rekawek.toxiproxy.model.ToxicDirection
import org.slf4j.event.Level

import scala.concurrent.duration.Duration

class UnrecoverableToxiproxyParticipantSequencerTests
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with ToxiproxyParticipantSynchronizerBase {

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(toxiProxy)

  override def proxyConf: () => ParticipantToSequencerPublicApi =
    () =>
      ParticipantToSequencerPublicApi(
        sequencer = "sequencer1",
        name = "participant1-to-sequencer1-cannot-recover",
      )

  override def environmentDefinition: EnvironmentDefinition =
    ToxiproxyHelpers.environmentDefinitionDefault
      .withSetup { implicit env =>
        env.runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters
            .propose_update(
              synchronizer.synchronizerId,
              _.update(reconciliationInterval = PositiveDurationSeconds.ofSeconds(1)),
            )
        )
        connectParticipantsToDa(proxyConf(), toxiProxy)
      }
      .withTeardown(_ =>
        ToxiproxyHelpers.removeAllProxies(toxiProxy.runningToxiproxy.controllingToxiproxyClient)
      )

  private lazy val expectedProblems = List(
    "GrpcServiceUnavailable: UNAVAILABLE",
    "GrpcClientGaveUp: DEADLINE_EXCEEDED",
    "GrpcClientGaveUp: CANCELLED/HTTP/2",
    "Timeout 10 seconds expired",
    "shutdown did not complete gracefully",
    "Waiting 1 minute before reconnecting",
    "Futures timed out",
    "Retry timeout has elapsed",
    "Retry attempt cancelled due to shutdown",
    "Task rejected due to shutdown",
    // Ignore this stray build warning, as it's harmless
    "Please set the environment variable SCALACTIC_FILL_FILE_PATHNAMES to yes at compile time to enable this feature",
    "io.grpc.StatusRuntimeException: CANCELLED: io.grpc.Context was cancelled without error", // accompanied by "Token refresh failed"
  )

  "The participant - sequencer connection" should {

    "fail a bong sensibly" when {
      "the participant-sequencer connection goes down indefinitely for one participant" in {
        implicit env =>
          a[CommandFailure] should be thrownBy
            loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
              bongWithKillConnection(getProxy, Duration.Inf),
              logs => checkLogs(expectedProblems)(logs),
            )
      }

      "traffic on the participant-sequencer connection is blocked indefinitely for one participant" in {
        implicit env =>
          a[CommandFailure] should be thrownBy
            loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
              ToxiproxyHelpers.bongWithNetworkFailure(
                getProxy,
                proxy =>
                  proxy.underlying
                    .toxics()
                    .timeout("upstream-pause-indefinite", ToxicDirection.UPSTREAM, 0),
                Duration.Inf,
              ),
              logs => checkLogs(expectedProblems)(logs),
            )
      }
    }
  }
}
