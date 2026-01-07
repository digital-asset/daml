// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy

import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.PositiveDurationSeconds
import com.digitalasset.canton.console.{CantonInternalError, CommandFailure}
import com.digitalasset.canton.integration.plugins.toxiproxy.UseToxiproxy.ToxiproxyConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.{
  ProxyConfig,
  RunningProxy,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers.{
  bongWithKillConnection,
  checkLogs,
  pingAndLog,
}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.FailedToDetermineLedgerTime
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.protocol.LocalRejectError.ConsistencyRejections.LockedContracts
import eu.rekawek.toxiproxy.model.ToxicDirection
import org.scalatest.Assertion
import org.slf4j.event.Level

import scala.concurrent.duration.*

/** Runs toxiproxy between a member and a database. The database is Postgres.
  */
trait ToxiproxyDatabase
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with EntitySyntax
    with AccessTestScenario {

  def proxyConf: ProxyConfig
  def connectionName: String
  // used for reliability test annotation - the component whose DB connection is deteriorated
  def component: String
  def expectedLogProblems: List[String] =
    List(
      "Failed to validate connection org.postgresql",
      "marked as broken because of SQLSTATE",
      LockedContracts.id,
      FailedToDetermineLedgerTime.id,
    )

  override def environmentDefinition: EnvironmentDefinition =
    ToxiproxyHelpers.environmentDefinitionDefault
      .withSetup { env =>
        env.runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters
            .propose_update(
              synchronizer.synchronizerId,
              _.update(reconciliationInterval = PositiveDurationSeconds.ofSeconds(1)),
            )
        )
        connectParticipants(env)
      }
      .addConfigTransforms(
        ConfigTransforms.enableReplicatedAllNodes*
      )

  val toxiProxy = new UseToxiproxy(ToxiproxyConfig(List(proxyConf)))

  def getProxy: RunningProxy = {
    val toxiproxy = toxiProxy.runningToxiproxy
    toxiproxy.getProxy(proxyConf.name).value
  }

  def connectParticipants(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    participant2.synchronizers.connect_local(sequencer1, alias = daName)
  }

  def runWithLogCheck[A](task: => A, additionalProblems: List[String] = List.empty): A = {
    val expected = expectedLogProblems ++ additionalProblems
    if (expected == List.empty)
      task
    else
      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        task,
        logs => checkLogs(expected)(logs),
      )
  }

  private def generateBongTag(outcome: String)(details: String) =
    ReliabilityTest(
      Component(name = component, setting = s"single non-replicated $component"),
      AdverseScenario(
        dependency = connectionName,
        details = details,
      ),
      Remediation(
        remediator = component,
        action =
          s"$component safely logs errors originating from $connectionName network issues and fully recovers its functionality once the connection is healthy again",
      ),
      outcome,
    )

  s"the $connectionName" should {
    val latencyMillis = 20L
    "keep successfully running a bong" when_ { outcome =>
      def successfulBongTag(details: String) = generateBongTag(outcome)(details)

      s"the $connectionName has high latency ($latencyMillis milliseconds)".taggedAs_(details =>
        successfulBongTag(details)
      ) in { implicit env =>
        def upstreamLatency(proxy: RunningProxy) =
          proxy.underlying
            .toxics()
            .latency("upstream-latency", ToxicDirection.UPSTREAM, latencyMillis)

        def downstreamLatency(proxy: RunningProxy) =
          proxy.underlying
            .toxics()
            .latency("downstream-latency", ToxicDirection.DOWNSTREAM, latencyMillis)

        logger.info(s"Bong with toxic starts")

        runWithLogCheck(
          ToxiproxyHelpers.bongWithToxic(getProxy, List(upstreamLatency, downstreamLatency), 2)
        )

      }

      s"the $connectionName has low bandwidth" in { implicit env =>
        lazy val rateKBperSecond = 100L
        def upstreamBandwidth(proxy: RunningProxy) =
          proxy.underlying
            .toxics()
            .bandwidth("upstream-bandwidth", ToxicDirection.UPSTREAM, rateKBperSecond)

        def downstreamBandwidth(proxy: RunningProxy) =
          proxy.underlying
            .toxics()
            .bandwidth("downstream-bandwidth", ToxicDirection.DOWNSTREAM, rateKBperSecond)

        runWithLogCheck(
          ToxiproxyHelpers.bongWithToxic(getProxy, List(upstreamBandwidth, downstreamBandwidth), 2)
        )
      }

      s"the $connectionName has high jitter and slices data into chunks separated by delay" in {
        implicit env =>
          val latencyMillis = 20L
          val sliceBytesAverage = 2000L
          val sliceDelayMicrosAverage = 1000L
          val sliceBytesVariation = 1500L

          def upstreamJitter(proxy: RunningProxy) =
            proxy.underlying
              .toxics()
              .latency("upstream-jitter", ToxicDirection.UPSTREAM, latencyMillis)
              .setJitter(latencyMillis)

          def downstreamJitter(proxy: RunningProxy) =
            proxy.underlying
              .toxics()
              .latency("downstream-jitter", ToxicDirection.DOWNSTREAM, latencyMillis)
              .setJitter(latencyMillis)

          def upstreamSlicer(proxy: RunningProxy) =
            proxy.underlying
              .toxics()
              .slicer(
                "upstream-slicer",
                ToxicDirection.UPSTREAM,
                sliceBytesAverage,
                sliceDelayMicrosAverage,
              )
              .setSizeVariation(sliceBytesVariation)

          def downstreamSlicer(proxy: RunningProxy) =
            proxy.underlying
              .toxics()
              .slicer(
                "downstream-slicer",
                ToxicDirection.DOWNSTREAM,
                sliceBytesAverage,
                sliceDelayMicrosAverage,
              )
              .setSizeVariation(sliceBytesVariation)

          runWithLogCheck(
            ToxiproxyHelpers.bongWithToxic(
              getProxy,
              List(upstreamJitter, downstreamJitter, upstreamSlicer, downstreamSlicer),
              levels = 2,
            )
          )
      }

      s"traffic on the $connectionName is blocked for 2 seconds".taggedAs_(details =>
        successfulBongTag(details)
      ) in { implicit env =>
        // Timeout of zero stops all data getting through
        runWithLogCheck(
          ToxiproxyHelpers.bongWithNetworkFailure(
            getProxy,
            proxy =>
              proxy.underlying
                .toxics()
                .timeout("upstream-pause-2sec", ToxicDirection.UPSTREAM, 0),
            2.seconds,
          ),
          additionalProblems = List(
            "Connection is closed", // Generic variant returned in SQLException
            "This connection has been closed", // PG-specific variant returned in PSQLException
            "UNAVAILABLE: SERVICE_NOT_RUNNING",
          ),
        )

        // TODO(i8625): also test that every command eventually leads to a completion and no command is executed twice.
        //  A bong is not suitable for testing this.
      }

      s"the $connectionName goes down for 2 seconds".taggedAs(
        successfulBongTag(
          "connection goes down for 2 seconds"
        )
      ) in { implicit env =>
        // The TCP session is immediately terminated with a `close` and cannot be re-established for 2 seconds
        ToxiproxyHelpers.bongWithKillConnection(
          getProxy,
          downFor = 2.seconds,
          levels = 3,
          recoveryCheck = pingAndLog(_),
        )
      }
    }

    "recover" when {
      s"the $connectionName goes down for 2 minutes" taggedAs_ (details =>
        ReliabilityTest(
          Component(name = component, setting = s"single non-replicated $component"),
          AdverseScenario(connectionName, details),
          Remediation(remediator = component, action = s"retries to reestablish $connectionName"),
          s"$component recovers after connection is restored",
        )
      ) in { implicit env =>
        import env.*
        val levels = 4

        // The `bong` command will fail due to timeouts
        val catchErrors: PartialFunction[Throwable, Assertion] = { case exn: CommandFailure =>
          logger.info(s"Bong command failed: $exn")
          succeed
        }

        // Timeout of zero stops all data getting through
        ToxiproxyHelpers.bongWithKillConnection(
          getProxy,
          downFor = 2.minutes,
          levels,
          recoveryCheck = env => {
            // Wait for connection timeout, so that connection timeouts occur before shutdown and can still be retried.
            logger.info("Waiting until connection timeout has elapsed...")
            Threading.sleep(
              sequencer1.config.storage.parameters.connectionTimeout.duration.toMillis
            )

            ToxiproxyHelpers.pingAndLog(env)
          },
          catchBongErrors = catchErrors,
        )
      }
    }

    val reasonableTime = 30.seconds
    s"shut down in reasonable time ($reasonableTime)" when {
      s"the $connectionName is down" in { implicit env =>
        import env.*
        val bongT = participant1.testing.bong(
          Set(participant1.id, participant2.id),
          timeout = 10.seconds,
          levels = 2,
        )
        logger.info(s"The system starts up happily: the bong is performed in $bongT")

        logger.info(s"The connection to the database goes down")
        val proxy = getProxy
        proxy.underlying.disable()

        logger.info(s"The system shuts down whilst the database is down")
        val start = Deadline.now
        try {
          ToxiproxyHelpers.closeMembers(proxy)
        } catch {
          case exn @ (_: CommandFailure | _: CantonInternalError) =>
            logger.info(s"Closing of members failed with exception", exn)
            // Ignore the command failure.
            // We are trying to test that shutdown does not spin forever when the connection is down.
            // So it is OK for shutdown to throw an exception, as long as it completes sufficiently quickly.
            ()
        }
        val time = Deadline.now - start

        logger.info(s"Shutdown took $time")
        time should be < reasonableTime

      // TODO(i8625): test that the system can be restarted and recovers into an operational state (i.e. ping)
      }
    }

    "fail a bong" when {
      // sanity-check that the toxiproxy setup for this test class is actually doing something
      s"the $connectionName goes down indefinitely" in { implicit env =>
        import env.*

        // This should succeed until the db connection is messed with
        participant1.testing.bong(
          Set(participant1.id, participant2.id),
          timeout = ToxiproxyHelpers.bongTimeout,
          levels = ToxiproxyHelpers.defaultBongLevel,
        )

        logger.info(s"Starting a bong before a network failure")
        a[CommandFailure] should be thrownBy bongWithKillConnection(
          getProxy,
          Duration.Inf,
        )
      }
    }
  }
}
