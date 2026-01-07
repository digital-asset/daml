// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.benchmarks

import better.files.File
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.TempDirectory
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.integration.plugins.{
  LocalPostgresDumpRestore,
  UseBftSequencer,
  UsePostgres,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.manual.ReplayTestCommon.*
import com.digitalasset.canton.integration.tests.manual.{
  BftSequencerPerformanceRunnerRecorder,
  PerformanceRunnerRecorder,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.sequencing.client.ReplayAction.SequencerSends
import com.digitalasset.canton.sequencing.client.ReplayConfig
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeBootstrap
import com.digitalasset.canton.topology.MediatorId
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.util.FutureInstances.*
import monocle.macros.syntax.lens.*
import org.scalatest.concurrent.PatienceConfiguration

import java.nio.file.Path
import scala.collection.mutable
import scala.concurrent.duration.*

/** Replays submissions to the Sequencer from the Participant and Mediator recorded by the embedded
  * [[PerformanceRunnerRecorder]] with a matching configuration, i.e., this benchmark is
  * self-sufficient. To get more "meaningful" results, increase the total number of cycles for the
  * test phase in the [[PerformanceRunnerRecorder]]. This will result in more events to replay.
  *
  * ==Manual runs==
  * Use the instructions from [[PerformanceRunnerRecorder]]. Note that metrics from replay are
  * exposed at port 19091 instead of 19090.
  *
  * Remember to run the recording phase just once for a certain configuration, and then disable it
  * with: `-Dreplay-tests.enable-recording=false`.
  *
  * Other configurable parameters:
  *   - Replay parallelism
  *   - Replay rate
  *   - Replay cycles
  *   - Replay timeout
  *
  * See the implementation for details.
  */
trait SequencerReplayBenchmark extends CommunityIntegrationTest with SharedEnvironment {

  private val SourceDirectory: File = File("replay")

  /** A timeout duration for initializing the database */
  private val InitTimeout: FiniteDuration = 2.minutes

  /** Timeout for waiting for the node to start and the replay transport to be made available */
  private val StartupTimeout: FiniteDuration = defaultPatience.timeout.plus(10.seconds)

  /** If the sequencer client transport has not witnessed an event within this period we will assume
    * that the sequencer is idle and will likely produce no more events.
    */
  private val IdleDuration: FiniteDuration = 10.seconds

  /** How frequently should we check if the idleness future has completed */
  private val IdleCheckInterval: FiniteDuration = 1.second

  private val MinNumberOfParticipants: Int = 2

  /** Whether to recreate a recording. Switch off if you already have a valid recording to save
    * time.
    */
  private val enableRecording: Boolean =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.enable-recording"))
      .forall(_.toBoolean)

  /** Whether to run an additional stage, where participants resubscribe from the beginning under
    * load. Enabled by default.
    */
  private val enableResubscriptionUnderLoad: Boolean =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.enable-resubscription"))
      .forall(_.toBoolean)

  /** How many (asynchronous) send requests per member can be concurrently made when replaying */
  private val sendReplayParallelism: Int =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.replay-parallelism"))
      .map(_.toInt)
      .getOrElse(2)

  /** How many send requests per second can be made (per member). `None` means "as fast as
    * possible".
    */
  private val sendReplayRatePerSecond: Option[Int] =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.replay-rate-per-second"))
      .map(_.toInt)

  /** How many times the same recording will be replayed (without dropping the data). Can be used to
    * lengthen the test.
    */
  private val sendReplayCycles: Int =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.replay-cycles"))
      .map(_.toInt)
      .getOrElse(1)

  /** A timeout duration for replaying submissions */
  private val replayTimeoutMinutes: Int =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.replay-timeout-minutes"))
      .map(_.toInt)
      .getOrElse(5)

  /** Tracing options. Disabled by default. */
  private val (tracingEnabled, tracingReportingPort, tracingSamplerRatio) =
    (
      Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.enable-tracing"))
        .exists(_.toBoolean),
      Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.tracing-reporting-port"))
        .map(_.toInt)
        .getOrElse(4317),
      Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.tracing-sampler-ratio"))
        .map(_.toDouble)
        .getOrElse(0.5),
    )

  /** Callback for the summary report. Uses `println` by default so that the CI doesn't complain,
    * and summaries are available in its output.
    */
  private val reportSummary: String => Unit = println(_)

  protected val postgresPlugin =
    new UsePostgres(
      loggerFactory,
      customDbNames = Some((identity, "_replay_tests")),
      customMaxConnectionsByNode = Some(_ => Some(numberOfDbConnectionsPerNode)),
    )
  registerPlugin(postgresPlugin)

  protected type StartingParticipantIndex = Int
  protected type NumberOfParticipantsPerGroup = Option[Int]
  protected val performanceRunnerRecorderFactory: (
      StartingParticipantIndex,
      NumberOfParticipantsPerGroup,
  ) => PerformanceRunnerRecorder

  protected val referenceBlockSequencerPlugin: Option[UseReferenceBlockSequencer[?]] =
    None

  // Use local tools as the dump files are not copied into the container.
  // Otherwise, the tests would fail with "No such file or directory".
  private lazy val postgresDumpRestore = LocalPostgresDumpRestore(postgresPlugin)

  private val participantNames = (1 to numberOfParticipants).map(idx => s"participant$idx")

  private val replayingParticipants: mutable.ArrayBuffer[ReplayingParticipant] =
    mutable.ArrayBuffer()

  override def afterAll(): Unit = {
    // ensure that we don't leave the replay config on the mediator class that could be erroneously picked up by other tests
    MediatorNodeBootstrap.replaySequencerConfig.set(PartialFunction.empty)
    replayingParticipants.foreach(_.close())
    super.afterAll()
  }

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        // We use lightweight replaying participants (see the implementation below for details).
        numParticipants = 0,
        numSequencers = numberOfSequencers,
        numMediators = numberOfMediators,
      )
      .clearConfigTransforms() // to disable globally unique ports
      .addConfigTransforms(configTransforms(prometheusHttpServerPort = Port.tryCreate(19091))*)
      .addConfigTransforms(
        _.focus(_.monitoring.tracing.tracer).replace(
          TracingConfig.Tracer(
            exporter =
              if (tracingEnabled) TracingConfig.Exporter.Otlp(port = tracingReportingPort)
              else TracingConfig.Exporter.Disabled,
            sampler = TracingConfig.Sampler.TraceIdRatio(ratio = tracingSamplerRatio),
          )
        ),
        // TODO(i26481): Enable new connection pool (test uses replay subscriptions)
        ConfigTransforms.disableConnectionPool,
      )
      .withManualStart

  /** Replays sends from the provided send directory. Returns a two element list with the sequencer
    * counters of the mediator and participant when the test stopped.
    */
  private def replaySends(replayDirectory: Path)(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    // make sure we don't overwrite recordings
    MediatorNodeBootstrap.recordSequencerInteractions.set(PartialFunction.empty)

    val mediatorSendConfigs: Map[String, SequencerSends] =
      (1 to numberOfMediators)
        .map(index => s"mediator$index" -> SequencerSends(loggerFactory, usePekko = true))
        .toMap

    // the replay configs need to be set upfront
    MediatorNodeBootstrap.replaySequencerConfig.set { case MediatorId(uid) =>
      val name = uid.identifier.str
      val sendsConfig = mediatorSendConfigs(name)
      ReplayConfig(replayDirectory, sendsConfig)
    }

    // start sequencers, mediators (with the replay transport), and participants (with the synchronizer disconnected)
    nodes.local.start()

    val sortedSequencers = sequencers.local.sortBy(_.name).toVector
    val participantSendConfigs = participantNames.zipWithIndex
      .map { case (name, idx) =>
        val recordingFileName =
          replayDirectory.toFile
            .listFiles()
            .filter(_.getName.startsWith(s"PAR::$name"))
            .head
            .getName
            .split('.')
            .head

        // Connect participants to sequencers assuming that there might be more participants than sequencers.
        // If there are less, not all sequencers will get submissions.
        val sequencerToConnect = sortedSequencers(idx % numberOfSequencers)

        val replayingParticipant = ReplayingParticipant
          .tryCreate(
            sequencerToConnect,
            replayDirectory,
            recordingFileName,
            futureSupervisor,
            postgresPlugin,
            wallClock,
            loggerFactory,
            timeouts,
          )
        replayingParticipants += replayingParticipant
        replayingParticipant.sendsConfig
      }

    // NOTE: throughout this block we'll hold instances for the mediator and participant
    // in a two item list where the mediator always comes first

    // get our replay transport
    val transports =
      (Seq[SequencerSends]() ++ mediatorSendConfigs.values ++ participantSendConfigs)
        .parTraverse(_.transport)
        .futureValue(
          PatienceConfiguration.Timeout(StartupTimeout),
          PatienceConfiguration.Interval(IdleCheckInterval),
        )

    // wait for the sequencer transports to read the events that were stored in the sequencer from the original performance test
    // when we've read all events this will provide us
    val startingTimestamps = transports
      .parTraverse(_.waitForIdle(IdleDuration))
      .futureValue(
        PatienceConfiguration.Timeout(replayTimeoutMinutes.minutes),
        PatienceConfiguration.Interval(IdleCheckInterval),
      )
      .map(_.finishedAtTimestamp)

    // start listening again from where we just stopped but now for capturing the events we've resent
    println(
      s"Replaying send requests at sequencer (starting from ${startingTimestamps.mkString(",")})"
    )

    val idlenessMonitors =
      transports.zip(startingTimestamps).map { case (transport, startingTimestamp) =>
        transport.waitForIdle(IdleDuration, startingTimestamp)
      }

    // replay all the sends
    val sendReports = transports
      .parTraverse(_.replay(sendReplayParallelism, sendReplayRatePerSecond, sendReplayCycles))
      .futureValue(
        PatienceConfiguration.Timeout(replayTimeoutMinutes.minutes),
        PatienceConfiguration.Interval(IdleCheckInterval),
      )

    // wait for us to become idle after all of these new sends
    println("Waiting for all events to be received")
    val eventsReceivedReports =
      idlenessMonitors.sequence
        .futureValue(
          PatienceConfiguration.Timeout(replayTimeoutMinutes.minutes),
          PatienceConfiguration.Interval(IdleCheckInterval),
        )

    // zippee ki-yay
    (mediators.local.map(_.name).sorted ++ participantNames)
      .zip(sendReports)
      .zip(eventsReceivedReports)
      .zip(
        transports.map(
          _.metricReport(env.environment.configuredOpenTelemetry.onDemandMetricsReader.read())
        )
      )
      .foreach { case (((name, sendReport), eventReport), _) =>
        reportSummary(s"Reports for $name:\n$sendReport\n$eventReport")
      }
  }

  private def resubscribeParticipantsFromBeginningUnderLoad()(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val participantSendConfigs = replayingParticipants.map(_.sendsConfig)

    val transports =
      participantSendConfigs.toSeq
        .parTraverse(_.transport)
        .futureValue(
          PatienceConfiguration.Timeout(StartupTimeout),
          PatienceConfiguration.Interval(IdleCheckInterval),
        )

    val startingTimestamps = transports
      .parTraverse(_.waitForIdle(IdleDuration))
      .futureValue(
        PatienceConfiguration.Timeout(replayTimeoutMinutes.minutes),
        PatienceConfiguration.Interval(IdleCheckInterval),
      )
      .map(_.finishedAtTimestamp)

    val replayReportsF =
      transports
        .zip(startingTimestamps)
        .map { case (transport, startingTimestamp) =>
          transport.waitForIdle(IdleDuration, startingTimestamp)
        }
        .sequence

    // replay all sends again to generate load
    transports
      .parTraverse(_.replay(sendReplayParallelism, sendReplayRatePerSecond, sendReplayCycles))
      .futureValue(
        PatienceConfiguration.Timeout(replayTimeoutMinutes.minutes),
        PatienceConfiguration.Interval(IdleCheckInterval),
      )

    reportSummary("Resubscribing from the beginning")
    val resubscriptionReportsF =
      transports.map(_.waitForIdle(IdleDuration, startFromTimestamp = None)).sequence

    // wait for becoming idle after receiving all events (including the ones from the above subscriptions)
    val resubscriptionReports =
      replayReportsF
        .zip(resubscriptionReportsF)
        .futureValue(
          PatienceConfiguration.Timeout(replayTimeoutMinutes.minutes),
          PatienceConfiguration.Interval(IdleCheckInterval),
        )
        ._2

    participantNames.zip(resubscriptionReports).foreach { case (participantName, report) =>
      reportSummary(s"Resubscription report for $participantName:\n$report")
    }
  }

  "Run the performance runner recorder" in { _ =>
    if (enableRecording) {
      val participantGroupsCount =
        if (numberOfParticipants > MinNumberOfParticipants) {
          require(
            numberOfParticipants % numberOfSequencers == 0,
            "Number of participants needs to be divisible by the number of sequencers",
          )
          numberOfParticipants / numberOfSequencers
        } else 1

      val numberOfParticipantsPerGroup =
        if (numberOfParticipants > MinNumberOfParticipants)
          numberOfSequencers
        else
          numberOfParticipants

      (1 to participantGroupsCount).foreach { participantGroupIndex =>
        // Example: participants = 16; sequencers = 4; start indexes = 1, 5, 9, 13
        val startingParticipantIndex =
          1 + ((participantGroupIndex - 1) * numberOfParticipantsPerGroup)
        performanceRunnerRecorderFactory(
          startingParticipantIndex,
          Some(numberOfParticipantsPerGroup),
        ).execute(durations = true, fullstacks = true)
      }
    } else {
      logger.warn("Skipping recording")
    }
  }

  "Initialize the DB" in { implicit env =>
    import env.*

    // The reference block sequencer uses a separate database.
    referenceBlockSequencerPlugin.foreach { plugin =>
      plugin
        .restoreDatabases(TempDirectory(dumpDirectory(SourceDirectory)), forceLocal = true)
        .futureValue
    }

    (mediators.local.map(_.name) ++ participantNames ++ sequencers.local.map(_.name))
      .foreach { nodeName =>
        postgresDumpRestore
          .restoreDump(nodeName, dumpPathOfNode(nodeName, SourceDirectory))
          .futureValue(
            PatienceConfiguration.Timeout(InitTimeout),
            PatienceConfiguration.Interval(IdleCheckInterval),
          )
      }
  }

  "Test" in { implicit env =>
    // we're intentionally not doing a warmup as the sequencer client metrics will capture everything regardless (not just from the test)
    // instead the initial portion of metrics should be discarded
    println("Running Sequencer Replay Test")
    replaySends(testDirectory(SourceDirectory).path)
  }

  "Resubscribe from beginning under load" in { implicit env =>
    if (enableResubscriptionUnderLoad) {
      resubscribeParticipantsFromBeginningUnderLoad()
    } else {
      logger.warn("Skipping resubscription under load")
    }
  }
}

class BftSequencerReplayBenchmark extends SequencerReplayBenchmark {
  private val useInMemoryStorageForBftOrderer: Boolean =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.bft-orderer-use-in-memory-storage"))
      .exists(_.toBoolean)

  override protected val performanceRunnerRecorderFactory
      : (StartingParticipantIndex, NumberOfParticipantsPerGroup) => PerformanceRunnerRecorder =
    (startingParticipantIndex, numberOfParticipantsPerGroup) =>
      new BftSequencerPerformanceRunnerRecorder(
        startingParticipantIndex,
        numberOfParticipantsPerGroup,
      )

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      shouldOverwriteStoredEndpoints = true,
      shouldUseMemoryStorageForBftOrderer = useInMemoryStorageForBftOrderer,
      shouldBenchmarkBftSequencer = true,
    )
  )
}

// Commented out to provide an implementation for manual runs while saving CI costs.
//
//class ReferenceSequencerReplayBenchmark extends SequencerReplayBenchmark {
//  override protected val performanceRunnerRecorder: PerformanceRunnerRecorder =
//    new ReferenceSequencerPerformanceRunnerRecorder()
//  override protected val referenceBlockSequencerPlugin
//      : Option[UseReferenceBlockSequencer[?]] =
//    Some(
//      new UseReferenceBlockSequencer[DbConfig.Postgres](
//        loggerFactory,
//        postgres = Some(postgresPlugin),
//      )
//    )
//
//  referenceBlockSequencerPlugin.foreach(registerPlugin(_))
//}
