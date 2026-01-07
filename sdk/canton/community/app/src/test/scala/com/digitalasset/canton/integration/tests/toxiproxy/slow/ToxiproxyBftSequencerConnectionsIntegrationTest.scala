// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import com.digitalasset.canton.admin.api.client.data.{
  GrpcSequencerConnection,
  ParticipantStatus,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{CommandFailure, InstanceReference}
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.UnknownContractSynchronizers
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.toxiproxy.{
  ParticipantToSequencerPublicApi,
  RunningProxy,
  UseToxiproxy,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.util.collection.SeqUtil
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, LoggerUtil, Mutex}
import eu.rekawek.toxiproxy.model.{Toxic, ToxicDirection}
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.mutable
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Future, Promise, blocking}
import scala.util.Random

/** This test checks that the sequencer connection pool properly recovers connections and
  * subscriptions when sequencers go down. This is a stress-test using toxiproxy to simulate failing
  * connections and focusing on a single member. The test also uses "cycles" (see `Cycle.daml` in
  * `CantonExamples`) instead of pings to verify connectivity to avoid the complexity and overhead
  * of pings.
  *
  * The environment is as follows:
  *   - 1 participant (actually 2, but participant2 is not used)
  *   - 4 sequencers
  *   - 1 mediator
  *
  * The participant and the mediator connect to all sequencers with a trust threshold of 2 and
  * initially a liveness margin of 0. The participant connections to the sequencers are controlled
  * by toxiproxy.
  *
  * The test uses "disruptors" to create harsh conditions: each disruptor is a background thread
  * that continuously picks a sequencer at random, applies a "timeout" toxiproxy on the connection
  * using random parameters, waits a random duration, and releases it.
  *
  * The main scenario is as follows:
  *   - Execute 5 initial cycles
  *   - Start 2 disruptors
  *   - Execute 100 cycles under disruptions
  *   - Stop the disruptors
  *   - Execute 5 final cycles
  *
  * The only failure tolerated during these scenarios are cycles failing due to the synchronizer
  * being unhealthy, which happens when the number of subscriptions temporarily goes under the trust
  * threshold. When this happens, the cycle is retried.
  *
  * This scenario is executed once with the participant using a liveness margin of 0, and once using
  * a liveness margin of 2. The expectation is that the time to execute the scenario is shorter with
  * a higher liveness margin, but this is currently not asserted by the test (the current test
  * timings as well as the long durations due to the failures mentioned above don't allow to see
  * this consistently).
  *
  * Finally, a similar scenario is executed with 3 disruptors, and executing only 5 cycles under
  * disruptions. In this case, it is expected that the commands will fail with other errors such as
  * timeouts. When it happens, we stop the disruptors, but verify that another 5 final cycles go
  * through normally when the participant has recovered.
  */
sealed trait ToxiproxyBftSequencerConnectionsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S4M1_Config
      .addConfigTransforms(ConfigTransforms.setConnectionPool(true))
      .withNetworkBootstrap { implicit env =>
        import env.*

        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = sequencers.local,
            mediators = Seq(mediator1),
            overrideMediatorToSequencers = Some(
              Map(
                mediator1 -> (sequencers.local,
                /* trust threshold */ PositiveInt.two, /* liveness margin */ NonNegativeInt.zero)
              )
            ),
          )
        )
      }

  private val random = {
    val seed = Random.nextLong()
    logger.debug(s"Seed for randomness = $seed")
    new Random(seed)
  }

  private lazy val proxyConfs = {
    // Ensure toxics have a unique name (with high probability), thereby avoiding conflicts in CI
    val uniqueId = Seq.fill(10)(('a' + random.nextInt(26)).toChar).mkString("")

    // NOTE: Update upper bound if the number of sequencers in the environment changes
    (1 to 4).map(index =>
      ParticipantToSequencerPublicApi(s"sequencer$index", name = s"toxi-$uniqueId-sequencer$index")
    )
  }
  private lazy val toxiProxyConf: UseToxiproxy.ToxiproxyConfig =
    UseToxiproxy.ToxiproxyConfig(proxyConfs)
  protected lazy val toxiproxyPlugin = new UseToxiproxy(toxiProxyConf)

  private lazy val proxies =
    proxyConfs.map(proxyConf => toxiproxyPlugin.runningToxiproxy.getProxy(proxyConf.name).value)

  private val NbIterations = 100

  private val commandId = new AtomicInteger(0)
  private def nextCommandId = commandId.incrementAndGet

  "BFT Synchronizer" must {
    "Connect participants to the synchronizer" in { implicit env =>
      import env.*

      val sequencerConnectionsToProxies = proxies.map(proxy =>
        GrpcSequencerConnection.tryCreate(
          s"http://${proxy.ipFromHost}:${proxy.portFromHost}",
          sequencerAlias = proxy.underlying.getName,
        )
      )

      val amplification = SubmissionRequestAmplification(
        factor = 20,
        patience = config.NonNegativeFiniteDuration.tryFromDuration(1.seconds),
      )

      clue("participant1 connects to the synchronizer") {
        participant1.synchronizers.connect_bft(
          sequencerConnectionsToProxies,
          sequencerTrustThreshold = PositiveInt.two,
          sequencerLivenessMargin = NonNegativeInt.zero,
          submissionRequestAmplification = amplification,
          synchronizerAlias = daName,
          physicalSynchronizerId = Some(daId),
        )
      }
    }

    def runCyclesUnderDisruptions(nbOfDisruptions: Int, maxTimeoutMillis: Int)(implicit
        env: TestConsoleEnvironment
    ): Double = {
      val disrupter = new SequencerDisrupter(nbOfDisruptions, maxTimeoutMillis = maxTimeoutMillis)

      clue("Run a warmup cycle") {
        runCyclesOnP1(1, "warmup", tolerateFailure = false)
      }

      clue("Run a few cycles before") {
        runCyclesOnP1(5, "before", tolerateFailure = false)
      }

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          disrupter.start()

          runCyclesOnP1(NbIterations, "under-disruption", tolerateFailure = true)

          disrupter.stopAndWaitUntilDone().futureValue

          clue("Run a few cycles after") {
            runCyclesOnP1(5, "after", tolerateFailure = false)
          }
        },
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq.empty,
          mayContain = expectedLogEntries,
        ),
      )
    }

    "Handle sequencer connections randomly going up and down (2 disruptions, liveness margin = 0)" in {
      implicit env =>
        runCyclesUnderDisruptions(nbOfDisruptions = 2, maxTimeoutMillis = 4000)
    }

    "Update configuration to use a liveness margin of 2" in { implicit env =>
      import env.*

      clue("participant1 disconnects, updates the config and reconnects") {
        participant1.synchronizers.disconnect_all()
        participant1.synchronizers.modify(
          synchronizerAlias = daName,
          _.focus(_.sequencerConnections).modify(_.withLivenessMargin(NonNegativeInt.two)),
        )
        participant1.synchronizers.reconnect_all()
      }
    }

    "Handle sequencer connections randomly going up and down (2 disruptions, liveness margin = 2)" in {
      implicit env =>
        runCyclesUnderDisruptions(nbOfDisruptions = 2, maxTimeoutMillis = 1)
    }

    "Recover after too many faulty sequencers cause failed commands (3 disruptions)" in {
      implicit env =>
        import env.*

        val disrupter = new SequencerDisrupter(nbOfDisruptions = 3)

        clue("Run a few cycles before") {
          runCyclesOnP1(5, "before", tolerateFailure = false)
        }

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            disrupter.start()

            // In this test, we don't strive to make the 5 cycles run but expect to quickly fail.
            // The focus of the test is to ensure that cycles run normally after full recovery.
            try {
              runCyclesOnP1(5, "under-disruption", tolerateFailure = false)
            } catch {
              case _: CommandFailure => logger.debug(s"Command failed")
            }

            disrupter.stopAndWaitUntilDone().futureValue

            clue("Wait until the participant has recovered...") {
              eventually() {
                participant1.health.status.trySuccess.connectedSynchronizers
                  .get(daId)
                  .value shouldBe ParticipantStatus.SubmissionReady(true)
              }
            }

            logger.debug(s"Cleaning up")
            cleanupCycles(
              participant1.adminParty,
              participant1,
              commandId = s"cycle-cleanup-$nextCommandId",
            )

            clue("Run a few cycles after") {
              runCyclesOnP1(5, "after", tolerateFailure = false)
            }
          },
          LogEntry.assertLogSeq(
            mustContainWithClue = Seq.empty,
            mayContain = expectedLogEntries ++ expectedLogEntriesForFailedRequests,
          ),
        )
    }

    "Disconnect participants from synchronizer" in { implicit env =>
      import env.*

      clue("Disconnect from synchronizer") {
        participant1.synchronizers.disconnect_all()
      }
    }
  }

  private def runCyclesOnP1(nb: Int, cycleType: String, tolerateFailure: Boolean)(implicit
      env: TestConsoleEnvironment
  ): Double = {
    val durations =
      (1 to nb).map(iteration => runCycleOnP1(s"$cycleType-#$iteration", tolerateFailure))
    val averageDuration =
      durations.foldLeft(Duration.Zero: Duration)(_ + _).toMillis.toDouble / durations.size
    logger.debug(s"=== Average duration of \"$cycleType\" cycles: $averageDuration ms")

    averageDuration
  }

  private def runCycleOnP1(cycleId: String, tolerateFailure: Boolean)(implicit
      env: TestConsoleEnvironment
  ): Duration = {
    import env.*

    def run(): Unit = clue(s">>> Run cycle \"$cycleId\"") {
      runCycle(
        participant1.adminParty,
        participant1,
        participant1,
        commandId = s"cycle-$cycleId-$nextCommandId",
      )
    }

    val timeBefore = System.nanoTime

    if (tolerateFailure)
      eventually(timeUntilSuccess = 60.seconds, maxPollInterval = 1.seconds) {
        try {
          run()
        } catch {
          case _: CommandFailure =>
            logger.debug(s"Command failed")
            eventually(timeUntilSuccess = 30.seconds, maxPollInterval = 1.seconds) {
              logger.debug("Attempting cleanup")
              try {
                cleanupCycles(
                  participant1.adminParty,
                  participant1,
                  commandId = s"cycle-cleanup-$nextCommandId",
                )
              } catch {
                case _: CommandFailure => fail()
              }
            }
            logger.debug(s"Cleanup successful")
            fail()
        }
      }
    else run()

    val elapsed = Duration.fromNanos(System.nanoTime - timeBefore)
    logger.debug(
      s"<<< Cycle \"$cycleId\" completed in ${LoggerUtil.roundDurationForHumans(elapsed)}"
    )

    elapsed
  }

  /** Bounds are inclusive */
  private class SequencerDisrupter(
      nbOfDisruptions: Int,
      minDurationMillis: Int = 1000,
      maxDurationMillis: Int = 10000,
      minTimeoutMillis: Int = 1, // 0 disables timeout but blocks all traffic
      maxTimeoutMillis: Int = 4000,
  )(implicit env: TestConsoleEnvironment) {
    private val isRunning = new AtomicBoolean(false)
    private val nbRunning = new AtomicInteger(0)
    private val doneP = Promise[Unit]()

    def start(): Unit = {
      logger.debug(s"Starting $nbOfDisruptions disrupters")
      isRunning.set(true)
      nbRunning.set(nbOfDisruptions)
      (1 to nbOfDisruptions).foreach(runner => run(runner, 1))
    }

    private val availableProxies = mutable.Set(proxies*)
    private val lock = new Mutex()

    private def run(disrupter: Int, iteration: Int): Unit =
      if (isRunning.get && !isClosing) {
        val durationOfDisruption = config.NonNegativeFiniteDuration.ofMillis(
          random.between(minDurationMillis.toLong, maxDurationMillis.toLong + 1)
        )

        val (pickedProxy, toxics) =
          pickSequencersAndActivateToxics(disrupter, iteration, durationOfDisruption)

        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
          env.environment.clock.scheduleAfter(
            { _ =>
              toxics.foreach(_.remove())
              blocking(lock.exclusive(availableProxies += pickedProxy))
              run(disrupter, iteration + 1)
            },
            durationOfDisruption.asJava,
          ),
          "running toxics",
        )
      } else {
        logger.debug(s"Disrupter #$disrupter terminated")
        if (nbRunning.decrementAndGet() == 0) doneP.trySuccess(())
      }

    private def pickSequencersAndActivateToxics(
        disrupter: Int,
        iteration: Int,
        durationOfDisruption: config.NonNegativeFiniteDuration,
    ): (RunningProxy, Seq[Toxic]) = {
      def pickTimeout(): Int =
        random.between(minTimeoutMillis, maxTimeoutMillis + 1)

      val pickedProxy = blocking(lock.exclusive {
        val pick = SeqUtil.randomSubsetShuffle(availableProxies.toIndexedSeq, 1, random).loneElement
        availableProxies -= pick
        pick
      })

      val timeout1 = pickTimeout()
      val timeout2 = pickTimeout()

      val toxics = clue(
        s"[Disruption #$disrupter.$iteration, duration=$durationOfDisruption]" +
          s" Activate toxic on ${pickedProxy.underlying.getName} " +
          s"with timeouts ${timeout1}ms downstream and ${timeout2}ms upstream"
      ) {
        Seq(
          pickedProxy.underlying
            .toxics()
            .timeout(
              s"${pickedProxy.underlying.getName}-timeout-down",
              ToxicDirection.DOWNSTREAM,
              timeout1.toLong,
            ),
          pickedProxy.underlying
            .toxics()
            .timeout(
              s"${pickedProxy.underlying.getName}-timeout-up",
              ToxicDirection.UPSTREAM,
              timeout2.toLong,
            ),
        )
      }

      (pickedProxy, toxics)
    }

    def stopAndWaitUntilDone(): Future[Unit] = {
      isRunning.set(false)
      doneP.future
    }
  }

  private val expectedLogEntries = Seq[LogEntry => Assertion](
    _.warningMessage should include("UNAVAILABLE/Network closed for unknown reason"),
    _.warningMessage should include("UNAVAILABLE/Channel shutdownNow invoked"),
    _.warningMessage should include("UNAVAILABLE/io exception"),
    _.warningMessage should include("UNAVAILABLE/Authentication token refresh error:"),
    _.warningMessage should include("Retry timeout has elapsed, giving up."),
    _.warningMessage should include("DEADLINE_EXCEEDED/CallOptions deadline exceeded"),
    _.warningMessage should include("Token refresh encountered error"),
    _.warningMessage should include(
      "Detected late processing (or clock skew) of batch with timestamp"
    ),
    _.warningMessage should include regex
      raw"FAILED_PRECONDITION/SEQUENCER_SUBMISSION_REQUEST_REFUSED\(.*\): The estimated sequencing time .* is already past",
    _.warningMessage should include regex raw"The operation 'request current time' was not successful. Retrying after .*No connection available",
    _.shouldBeCommandFailure(UnknownContractSynchronizers),
    _.errorMessage should include("Failed clue"),
  )

  private val expectedLogEntriesForFailedRequests = Seq[LogEntry => Assertion](
    _.warningMessage should include regex raw"Response message for request \[RequestId\(.*\)\] timed out at",
    _.errorMessage should include(
      "ABORTED/MEDIATOR_SAYS_TX_TIMED_OUT(2,0): Rejected transaction as the mediator did not receive sufficient confirmations within the expected timeframe"
    ),
  )
}

class ToxiproxyBftSequencerConnectionsIntegrationTestDefault
    extends ToxiproxyBftSequencerConnectionsIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(toxiproxyPlugin)
}
