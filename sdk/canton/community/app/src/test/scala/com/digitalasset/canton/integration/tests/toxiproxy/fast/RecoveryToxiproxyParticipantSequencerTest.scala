// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.fast

import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.config.PositiveDurationSeconds
import com.digitalasset.canton.integration.plugins.toxiproxy.ParticipantToSequencerPublicApi
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers.bongWithToxic
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyParticipantSynchronizerHelpers.connectParticipantsToDa
import com.digitalasset.canton.integration.tests.toxiproxy.{
  ToxiproxyHelpers,
  ToxiproxyParticipantSynchronizerBase,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.lifecycle.HasSynchronizeWithReaders
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.protocol.LocalRejectError.ConsistencyRejections.LockedContracts
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import eu.rekawek.toxiproxy.model.ToxicDirection
import org.slf4j.event.Level

import scala.concurrent.duration.*

class RecoveryToxiproxyParticipantSequencerTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with ToxiproxyParticipantSynchronizerBase
    with AccessTestScenario {

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(toxiProxy)

  override def proxyConf: () => ParticipantToSequencerPublicApi =
    () =>
      ParticipantToSequencerPublicApi(
        sequencer = "sequencer1",
        name = "participant1-to-sequencer1-recovery",
      )

  override def environmentDefinition: EnvironmentDefinition =
    ToxiproxyHelpers.environmentDefinitionDefault.withSetup { implicit env =>
      env.runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
        owner.topology.synchronizer_parameters
          .propose_update(
            synchronizer.synchronizerId,
            _.update(reconciliationInterval = PositiveDurationSeconds.ofSeconds(1)),
          )
      )
      connectParticipantsToDa(proxyConf(), toxiProxy)
    }

  private def generateBongTag(details: String) =
    ReliabilityTest(
      Component(
        name = "Bong application",
        setting = "connected to single non-replicated participant",
      ),
      AdverseScenario(
        "participant-sequencer connection",
        details,
      ),
      Remediation(
        remediator = "participant",
        action =
          s"participants safely handles errors originating from participant-sequencer network issues and fully recovers its functionality once the connection is healthy again",
      ),
      "bong can continue after connection recovers",
    )

  "The participant - sequencer connection" should {

    "keep successfully running a bong" when {
      s"the participant-sequencer connection has high latency for one participant" in {
        implicit env =>
          val latencyMillis = 2000L
          bongWithToxic(
            getProxy,
            List(proxy =>
              proxy.underlying
                .toxics()
                .latency("upstream-latency", ToxicDirection.DOWNSTREAM, latencyMillis)
            ),
            2,
            cleanClose = false,
          )
      }

      val rateKBperSecond = 20L
      s"the participant-sequencer connection has low bandwidth ($rateKBperSecond KB/s) for one participant" in {
        implicit env =>
          bongWithToxic(
            getProxy,
            List(proxy =>
              proxy.underlying
                .toxics()
                .bandwidth("upstream-bandwidth", ToxicDirection.UPSTREAM, rateKBperSecond)
            ),
            2,
            cleanClose = false,
          )
      }

      "the participant-sequencer connection has high jitter for one participant" in {
        implicit env =>
          val latencyMillis = 1000L
          val jitterMillis = 1000L
          bongWithToxic(
            getProxy,
            List(proxy =>
              proxy.underlying
                .toxics()
                .latency("upstream-jitter", ToxicDirection.UPSTREAM, latencyMillis)
                .setJitter(jitterMillis)
            ),
            2,
            cleanClose = false,
          )
      }

    }

    "the participant-sequencer connection goes down for 2 seconds for one participant" taggedAs_ (
      scenario => generateBongTag(scenario)
    ) in { implicit env =>
      // The TCP session is immediately terminated with a `close` and cannot be re-established for 2 seconds

      val expectedWarnings = List(
        "GrpcServiceUnavailable: UNAVAILABLE",
        // A ping transaction may be submitted that refers to locked contracts
        // As the participant-sequencer connection is being blocked
        LockedContracts.code.id,
        // The connection issues may cause a slow shutdown
        HasSynchronizeWithReaders.forceShutdownStr,
        // Response messages sent to the sequencer during the outage may time out
        "Response message for request .* timed out",
        // Some submissions may have timed out
        "Submission timed out at",
        LostSequencerSubscription.id,
        "RequestFailed\\(No connection available\\)",
      ).map(str =>
        // Allow the string to appear anywhere in the WARN statement
        s"[\\s\\S]*$str[\\s\\S]*"
      )

      loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.WARN))(
        ToxiproxyHelpers.bongWithKillConnection(getProxy, 2.seconds, cleanClose = false),
        logs =>
          forAll(logs) { x =>
            assert(
              expectedWarnings.exists(msg => x.warningMessage.matches(msg)),
              s"line $x contained unexpected warning",
            )
          },
      )
    }

    "traffic on the participant-synchronizer connection is blocked for 2 seconds for one participant" taggedAs_ (
      scenario => generateBongTag(scenario)
    ) in { implicit env =>
      // The TCP session is not immediately terminated, and traffic is blocked for 2 seconds. The session may,
      // however, end up getting terminated due to timeouts.

      // Timeout of zero stops all data getting through
      ToxiproxyHelpers.bongWithNetworkFailure(
        getProxy,
        proxy => proxy.underlying.toxics().timeout("upstreampause2sec", ToxicDirection.UPSTREAM, 0),
        1.seconds,
      )
    }

  }
}
