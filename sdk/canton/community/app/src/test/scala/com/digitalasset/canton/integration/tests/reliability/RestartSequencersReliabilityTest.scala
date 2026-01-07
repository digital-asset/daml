// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.reliability

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseSharedStorage}
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest
import com.digitalasset.canton.integration.{EnvironmentDefinition, TestConsoleEnvironment}
import com.digitalasset.canton.lifecycle.ShutdownFailedException
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.resource.DbLockedConnectionPool.NoActiveConnectionAvailable
import org.scalatest.Assertion

import scala.concurrent.duration.*
import scala.concurrent.{Future, TimeoutException}
import scala.util.Try

// TODO(#16089): Currently this test cannot work due to the issue
class RestartSequencersReliabilityTest
    extends ReliabilityPerformanceIntegrationTest
    with BasePerformanceIntegrationTest
    with ReliabilityTestSuite {

  private val sequencer1Name = "sequencer1"
  private val sequencer2Name = "sequencer2"
  private val sequencer3Name = "sequencer3"

  /** external-ha-sequencers.conf is based on the typical single synchronizer topology used for the
    * perf tests, but unsurprisingly adds many external ha sequencers.
    */
  override protected lazy val baseEnvironmentConfig: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = 3,
        numMediators = 1,
      )
      .withManualStart

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    UseSharedStorage.forSequencers(
      sequencer1Name,
      Seq(sequencer2Name, sequencer3Name),
      loggerFactory,
    )
  )

  override protected def baseSetup(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    participants.local.start()
    participant1.synchronizers.connect_multi(daName, Seq(sequencer2, sequencer3))
  }

  private def nodeOutage(
      node1: LocalInstanceReference,
      node2: LocalInstanceReference,
      hasCompleted: () => Boolean,
  )(index: Int): Unit =
    if (!hasCompleted()) {
      // alternate between nodes to take down
      val node = if (index % 2 == 0) node1 else node2

      logger.debug(s"Stopping ${node.name}")
      // This can fail, as the node is busy during shutdown.
      Try(node.stop())
      Threading.sleep(7.seconds.toMillis)
      logger.debug(s"Starting ${node.name}")
      node.start()
      node.health.wait_for_initialized()
    } else {
      checkPerfRunnerCompletion(index)
    }

  "kill a sequencer while the runners are running".taggedAs(
    ReliabilityTest(
      Component(
        name = "Participant",
        setting = "connected to replicated sequencer",
      ),
      AdverseScenario(
        dependency = "Sequencer node",
        details = "sequencer node process is forcefully killed while running",
      ),
      Remediation(
        remediator = "participant",
        action =
          "gRPC client-side load balancer of the participant directs requests at running sequencer nodes",
      ),
      outcome = "participant can process transactions through the running replicated sequencers",
    )
  ) ignore { implicit env => // TODO(#16089): Re-enable this test
    import env.*
    runWithFault(participant1.name, participant1.config.ledgerApi.port) { hasCompleted =>
      (1 to 10).foreach(nodeOutage(sequencer2, sequencer3, hasCompleted))
    }
  }

  "turn off all sequencers and then turn them back on".taggedAs(
    ReliabilityTest(
      Component(
        name = "Participant",
        setting = "connected to replicated sequencer",
      ),
      AdverseScenario(
        dependency = "Sequencer node",
        details = "all sequencer node processes are forcefully killed while running",
      ),
      Remediation(
        remediator = "participant",
        action =
          "gRPC client-side load balancer successfully reconnects once all sequencers resume operations",
      ),
      outcome = "participant can process transactions through the running replicated sequencers",
    )
  ) ignore { implicit env => // TODO(#16089): Re-enable this test
    import env.*

    participant2.synchronizers.connect_multi(daName, Seq(sequencer1, sequencer2, sequencer3))

    clue("participants can ping") {
      participant1.health.maybe_ping(participant2) shouldBe defined
    }
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        clue(s"shutting down all sequencers") {
          sequencers.local.foreach(_.stop())
        }

        // wait 10s such that when the sequencers come back online, they all think
        // that their colleagues are offline
        Threading.sleep(10000)
        forAll(sequencers.local) { seq =>
          seq.is_running shouldBe false
        }

        clue(s"restarting all sequencers") {
          Future
            .sequence(
              sequencers.local.map(s =>
                Future {
                  s.start()
                }
              )
            )
            .futureValue
        }

        clue(s"participants can ping again") {
          eventually() {
            participant1.health.maybe_ping(participant2) shouldBe defined
          }
        }
      },
      seq => forAll(seq)(acceptableErrorOrWarning),
    )

  }

  override protected def acceptableErrorOrWarning(entry: LogEntry): Assertion =
    inside(entry) {
      case LogEntry(
            _,
            _,
            _,
            Some(_: ShutdownFailedException | _: TimeoutException | _: NoActiveConnectionAvailable),
            _,
          ) =>
        succeed

      case LogEntry(_, _, message, _, _) =>
        message should (
          messageIncludesOneOf(
            "Sequencer shutting down",
            "shutdown did not complete gracefully in allotted 3 seconds",
          ) or
            matchAcceptableErrorOrWarnMessage
        )
    }
}
