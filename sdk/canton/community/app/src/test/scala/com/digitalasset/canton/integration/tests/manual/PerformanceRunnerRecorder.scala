// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import better.files.File
import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  Filters,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.{
  TransactionWrapper,
  UpdateWrapper,
}
import com.digitalasset.canton.admin.api.client.data.SequencerConnections
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{LocalSequencerReference, ParticipantReference}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  LocalPostgresDumpRestore,
  UseBftSequencer,
  UsePostgres,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.manual.PerformanceRunnerRecorder.*
import com.digitalasset.canton.integration.tests.manual.ReplayTestCommon.*
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.networking.grpc.ErrorLoggingStreamObserver
import com.digitalasset.canton.performance.PartyRole.{
  DvpIssuer,
  DvpTrader,
  Master,
  MasterDynamicConfig,
}
import com.digitalasset.canton.performance.model.java.orchestration.runtype
import com.digitalasset.canton.performance.{
  Connectivity,
  PartyRole,
  PerformanceRunner,
  PerformanceRunnerConfig,
  RateSettings,
}
import com.digitalasset.canton.sequencing.client.RecordingConfig
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeBootstrap
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.topology.{ForceFlag, ForceFlags, MediatorId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.{TempDirectory, config}
import org.scalatest.concurrent.PatienceConfiguration

import java.time.{Duration as JDuration, Instant}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.*

/** Run the performance runner and record events for later replay. Note that the recordings are
  * valid for approximately 24 hours due to the hardcoded topology timestamp tolerance.
  *
  * ==Database setup==
  *
  * If not running against a containerized Postgres instance, make sure to export the following
  * variables to point to the database:
  * {{{
  * export CI=true
  * export POSTGRES_USER=test
  * export POSTGRES_PASSWORD=supersafe
  * export POSTGRES_DB=postgres
  * }}}
  *
  * If the database is remote, additionally set this environment variable:
  * {{{export POSTGRES_HOST=db-testing.da-int.net}}}
  *
  * Make sure that Postgres is installed and the user and database from above do exist.
  * {{{
  * $ psql postgres
  * postgres=# create user test with password 'supersafe';
  * CREATE ROLE
  * postgres=# ALTER USER test SUPERUSER;
  * ALTER ROLE
  * postgres=# exit
  * }}}
  *
  * ==Manual runs==
  *
  * Tweak parameters defined in [[ReplayTestCommon]] as well as
  * `-Dscala.concurrent.context.numThreads` depending on your number of cores and Canton nodes.
  *
  * The currently recommended settings for 8/8/8-node runs (participants/sequencers/mediators) on
  * `canton-testing` machines are:
  * {{{
  *   export SBT_OPTS="-Xmx60G -Xms60G -Dreplay-tests.num-participants=8 -Dreplay-tests.num-sequencers-mediators=8 -Dreplay-tests.enable-prometheus-metrics=true -Dreplay-tests.dvp-traders-per-participant=5 -Dscala.concurrent.context.numThreads=30 -Dreplay-tests.num-db-connections-per-node=5 -Dreplay-tests.replay-cycles=10 -Dreplay-tests.replay-timeout-minutes=15"
  *   export LOG_LEVEL_CANTON=WARN
  * }}}
  *
  * To investigate metrics using Grafana, enable metrics with a system property defined in
  * [[ReplayTestCommon]] so that they can be scraped. To set up Prometheus and Grafana, you can run
  * the observability stack from one of our examples:
  *   1. Go to the
  *      `community/synchronizer/src/main/scala/com/digitalasset/canton/synchronizer/sequencer/block/bftordering/examples/observability`
  *      directory.
  *   1. Add `network_mode: "host"` to `prometheus`, `grafana`, and `postgres-exporter` in
  *      `docker-compose-observability.yml` (make sure your Docker implementation supports "host"
  *      network).
  *   1. For Postgres metrics/dashboard, change the following environment variables of the
  *      postgres-exporter in `docker-compose-observability.yml`:
  *      {{{
  *      DATA_SOURCE_USER: "test"
  *      DATA_SOURCE_PASS: "supersafe"
  *      DATA_SOURCE_URI: "localhost:5432/postgres?sslmode=disable"
  *      }}}
  *      The above assumes that the database is available locally at port 5432, e.g., using `ssh -L
  *      5432:localhost:5432 _user_@db-testing.da-int.net`. Configuring the prometheus-exporter for
  *      a containerized Postgres instance is more difficult due to dynamic ports.
  *   1. Change `canton`, `prometheus`, `grafana`, and `postgres` URLs to use `localhost` in
  *      `prometheus.yml` and `datasources.yml` files.
  *   1. Run `docker compose -f docker-compose-observability.yml up`.
  *
  * For more information, check the README file.
  *
  * If the recorder is run remotely, you can ssh to it with: `ssh _user_@_host_`. To expose metrics
  * port 19090 on the local machine add `-L 19090:localhost:19090`. To expose jaeger reporting port
  * from the local machine to the remote machine (SSH reverse tunnel) add `-R 4317:localhost:4317`.
  *
  * ==Grouping participants==
  *
  * Recordings can be made in rounds by grouping participants. It's especially useful for large
  * numbers of participants that would be difficult to run all at once.
  */
trait PerformanceRunnerRecorder extends CommunityIntegrationTest with SharedEnvironment {

  private val TargetDirectory: File = File("replay")

  /** Whether to enable recording. Switch off if you want to measure performance without the
    * recording overhead.
    */
  private val enableRecording: Boolean =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.enable-recording"))
      .forall(_.toBoolean)

  /** Total number of cycles for the test phase. Increase to get more meaningful results. */
  private val testTotalCycles: Int =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.total-cycles"))
      .map(_.toInt)
      .getOrElse(200)

  /** Overall timeout for doing a recording */
  private val Timeout: FiniteDuration = 10.minutes

  /** Overall timeout for making a database dump */
  private val DumpTimeout: FiniteDuration = 1.minute

  /** Maximum poll interval for checking completion */
  private val MaxPollInterval: FiniteDuration = 5.seconds

  /** Interval between throughput reports */
  private val ReportThroughputEveryNumTx: Int = 1000

  /** Minimum total cycles number accepted by the performance runner */
  private val MinimumTotalCycles = 2

  /** Callback for reporting the performance runner status */
  private val reportStatus: String => Unit = logger.info(_)

  protected val startingParticipantIndex: Int = 1
  protected val numberOfParticipantsPerGroup: Option[Int] = None

  private val MasterNames: Seq[String] = Seq(
    s"master-init-$startingParticipantIndex",
    s"master-warmup-$startingParticipantIndex",
    s"master-test-$startingParticipantIndex",
  )

  private def masterRole(name: String, totalCycles: Int): Master = Master(
    name,
    runConfig = MasterDynamicConfig(
      totalCycles = totalCycles,
      reportFrequency = Math.max(totalCycles / 10, 1),
      runType = new runtype.DvpRun(
        1000, // Be generous here so that we won't throttle due to a shortage of assets.
        0,
        0,
        0,
      ),
    ),
  )

  private val NumberOfIssuersPerParticipant: Int = 1
  private val numberOfTradersPerParticipant: Int =
    Option(System.getProperty(s"$ReplayTestSystemPropertyPrefix.dvp-traders-per-participant"))
      .map(_.toInt)
      .getOrElse(1)

  private def commonRolesOfParticipant(
      participantReference: ParticipantReference
  ): Set[PartyRole] = {
    val issuers = (1 to NumberOfIssuersPerParticipant).map(i =>
      DvpIssuer(s"${participantReference.name}-issuer$i", settings = RateSettings.defaults)
    )
    val traders = (1 to numberOfTradersPerParticipant).map(i =>
      DvpTrader(s"${participantReference.name}-trader$i", settings = RateSettings.defaults)
    )

    (issuers ++ traders).toSet
  }

  private val firstParticipantName = s"participant$startingParticipantIndex"

  protected val postgresPlugin =
    new UsePostgres(
      loggerFactory,
      customDbNames = Some((identity, "_replay_tests")),
      customMaxConnectionsByNode = Some(_ => Some(numberOfDbConnectionsPerNode)),
    )
  registerPlugin(postgresPlugin)

  protected val referenceBlockSequencerPlugin: Option[UseReferenceBlockSequencer[?]] =
    None

  private lazy val postgresDumpRestore = LocalPostgresDumpRestore(postgresPlugin)

  override def environmentDefinition: EnvironmentDefinition = {
    val baseEnvDefinition = EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = numberOfParticipantsPerGroup.getOrElse(numberOfParticipants),
        numSequencers = numberOfSequencers,
        numMediators = numberOfMediators,
        startingParticipantIndex = startingParticipantIndex,
      )
      .clearConfigTransforms() // to disable globally unique ports
      .addConfigTransforms(configTransforms()*)

    if (isFirstRound) {
      baseEnvDefinition
        .withNetworkBootstrap { implicit env =>
          import env.*

          val bootstrapMediatorsToThreshold =
            if (useSeparateMediatorGroups) {
              // When using separate mediator groups, other mediators need to be set up manually.
              Seq(mediator1) -> PositiveInt.one
            } else {
              // Note that in this case, we rely on the default sequencer connection logic.
              mediators.all -> PositiveInt.tryCreate(
                OrderingTopology.weakQuorumSize(numberOfSequencers)
              )
            }

          new NetworkBootstrapper(
            NetworkTopologyDescription(
              daName,
              synchronizerOwners = sequencers.local,
              synchronizerThreshold = PositiveInt.one,
              sequencers = sequencers.local,
              mediators = bootstrapMediatorsToThreshold._1,
              mediatorThreshold = bootstrapMediatorsToThreshold._2,
            )
          )
        }
        .withSetup { implicit env =>
          import env.*

          val sortedSequencers = sequencers.local.sortBy(_.name).toVector
          setUpParticipants(sortedSequencers)

          // Ultimately, increases `sequencerTopologyTimestampTolerance` to avoid `SEQUENCER_TOPOLOGY_TIMESTAMP_TOO_EARLY` rejections,
          //  i.e., makes the recording valid for approximately 24 hours.
          sequencer1.topology.synchronizer_parameters.propose_update(
            sequencer1.synchronizer_id,
            _.update(
              confirmationResponseTimeout =
                config.NonNegativeFiniteDuration.tryFromDuration(24.hours)
            ),
            force = ForceFlags(ForceFlag.AllowOutOfBoundsValue),
          )
          // Avoids `SEQUENCER_SUBMISSION_REQUEST_REFUSED` rejections due to calculating a max sequencing time upper
          //  bound based on `sequencerAggregateSubmissionTimeout`.
          sequencer1.topology.synchronizer_parameters.propose_update(
            sequencer1.synchronizer_id,
            _.update(
              sequencerAggregateSubmissionTimeout = config.NonNegativeFiniteDuration
                .tryFromDuration(Duration(Long.MaxValue, NANOSECONDS))
            ),
            force = ForceFlags(ForceFlag.AllowOutOfBoundsValue),
          )

          if (useSeparateMediatorGroups) {
            mediators.local
              .sortBy(_.name)
              .zip(sortedSequencers)
              .zipWithIndex
              .drop(1)
              .foreach { case ((mediator, sequencer), index) =>
                sequencer.topology.transactions
                  .load(
                    mediator.topology.transactions.identity_transactions(),
                    synchronizer1Id,
                    ForceFlag.AlienMember,
                  )
                sequencer.topology.mediators.propose(
                  synchronizer1Id,
                  threshold = PositiveInt.one,
                  active = Seq(mediator.id),
                  group = NonNegativeInt.tryCreate(index),
                )
                mediator.setup
                  .assign(
                    synchronizer1Id,
                    SequencerConnections.single(sequencer.sequencerConnection),
                  )
              }
          }
        }
    } else {
      baseEnvDefinition.withManualStart
        .withSetup { implicit env =>
          import env.*

          // Dumps are made later in the test. They are restored in rounds higher than 1.
          (mediators.local.map(_.name) ++ sequencers.local.map(_.name))
            .foreach { nodeName =>
              postgresDumpRestore
                .restoreDump(nodeName, dumpPathOfNode(nodeName, TargetDirectory))
                .futureValue
            }

          sequencers.local.start()
          sequencers.local.foreach(_.health.wait_for_initialized())
          mediators.local.start()
          mediators.local.foreach(_.health.wait_for_initialized())
          participants.local.start()
          participants.local.foreach(_.health.wait_for_running())

          val sortedSequencers = sequencers.local.sortBy(_.name).toVector
          setUpParticipants(sortedSequencers)
        }
    }
  }

  private def isFirstRound = startingParticipantIndex == 1

  private def setUpParticipants(
      sortedSequencers: Vector[LocalSequencerReference]
  )(implicit env: TestConsoleEnvironment) = {
    import env.*

    // Connect participants to sequencers assuming that there might be more participants than sequencers.
    // If there are less, not all sequencers will get submissions.
    participants.local.sortBy(_.name).zipWithIndex.foreach { case (participant, index) =>
      participant.synchronizers
        .connect_local(sortedSequencers(index % numberOfSequencers), daName)
    }

    participants.local.dars.upload(PerformanceTestPath)
  }

  private def withRunners[A](masterName: String, totalCycles: Int)(
      body: TestConsoleEnvironment => List[PerformanceRunner] => A
  ): TestConsoleEnvironment => A = { implicit env =>
    import env.*

    val firstParticipant = lp(firstParticipantName)
    val otherParticipants = participants.local.filter(_ != firstParticipant).toList

    val runners = {
      val masterRunner =
        createPerformanceRunner(
          firstParticipant,
          sequencer1.synchronizer_id,
          masterName,
          totalCycles,
          isMaster = true,
        )
      val otherRunners =
        otherParticipants.map(
          createPerformanceRunner(
            _,
            sequencer1.synchronizer_id,
            masterName,
            totalCycles,
            isMaster = false,
          )
        )
      masterRunner :: otherRunners
    }

    try {
      body(env)(runners)
    } finally {
      runners.foreach(_.close())
    }
  }

  private def createPerformanceRunner(
      p: ParticipantReference,
      baseSynchronizerId: SynchronizerId,
      masterName: String,
      totalCycles: Int,
      isMaster: Boolean,
  )(implicit ec: ExecutionContextExecutor): PerformanceRunner = {
    val localRoles =
      if (isMaster) commonRolesOfParticipant(p) + masterRole(masterName, totalCycles)
      else commonRolesOfParticipant(p)
    val connectivity = Connectivity(name = p.name, port = p.config.clientLedgerApi.port)
    val config =
      PerformanceRunnerConfig(masterName, localRoles, connectivity, baseSynchronizerId)
    new PerformanceRunner(config, _ => NoOpMetricsFactory, loggerFactory)
  }

  private def runUntilCompletion(
      runners: List[PerformanceRunner]
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    monitoringThroughput(
      participants.all,
      ReportThroughputEveryNumTx,
      logger,
      logger.info(_),
      _ => (),
    ) {
      val doneF = runners.parTraverse(r => EitherT(r.startup()))

      try {
        eventually(Timeout, MaxPollInterval) {
          reportStatus(s"Status reports:${runners.flatMap(_.status()).mkString("\n", "\n", "\n")}")

          doneF.value.isCompleted shouldBe true
          doneF.futureValue
        }
      } finally {
        runners.foreach(_.close())
      }
    }
  }

  "Initialize the ledger" in withRunners(MasterNames.head, MinimumTotalCycles) {
    implicit env => runners =>
      import env.*

      val firstParticipant = lp(firstParticipantName)

      // We need to do this up front, to include master parties of subsequent runs into the DB dump.
      MasterNames.foreach(n => firstParticipant.parties.enable(n))

      runUntilCompletion(runners)

      nodes.local.stop()
  }

  "Store a db dump" in { implicit env =>
    import env.*

    if (enableRecording) {
      if (isFirstRound) {
        dumpDirectory(TargetDirectory).delete(swallowIOExceptions = true)
        dumpDirectory(TargetDirectory).toJava.mkdirs()
      }

      // The reference block sequencer uses a separate database.
      referenceBlockSequencerPlugin.foreach { plugin =>
        plugin
          .dumpDatabases(TempDirectory(dumpDirectory(TargetDirectory)), forceLocal = true)
          .futureValue(timeout = PatienceConfiguration.Timeout(DumpTimeout))
      }

      // Dumps are overwritten in subsequent rounds and eventually contain full init data (topology state, DARs)
      //  for the replay. Note that they are made before writing test data (even in higher rounds). This prevents them
      //  from growing fast.
      nodes.local.foreach(node =>
        postgresDumpRestore
          .saveDump(node, dumpPathOfNode(node.name, TargetDirectory))
          .futureValue(timeout = PatienceConfiguration.Timeout(DumpTimeout))
      )
    }
  }

  "run a warmup test" in withRunners(MasterNames(1), MinimumTotalCycles) {
    implicit env => runners =>
      import env.*

      if (enableRecording) {
        if (isFirstRound) {
          warmupDirectory(TargetDirectory).delete(swallowIOExceptions = true)
          MediatorNodeBootstrap.recordSequencerInteractions.set { case MediatorId(_) =>
            RecordingConfig(warmupDirectory(TargetDirectory).path)
          }
        } else {
          // We don't record submissions from Mediators in rounds higher than 1 for simplicity.
          MediatorNodeBootstrap.replaySequencerConfig.set(PartialFunction.empty)
        }
      }

      nodes.local.start()

      if (enableRecording) {
        participants.local.foreach(
          _.underlying.value.recordSequencerInteractions
            .set(Some(RecordingConfig(warmupDirectory(TargetDirectory).path)))
        )
      }

      participants.all.synchronizers.reconnect_all()

      runUntilCompletion(runners)

      nodes.local.stop()
  }

  "run the performance test" in withRunners(MasterNames(2), testTotalCycles) {
    implicit env => runners =>
      import env.*

      if (enableRecording) {
        if (isFirstRound) {
          testDirectory(TargetDirectory).delete(swallowIOExceptions = true)
          MediatorNodeBootstrap.recordSequencerInteractions.set { case MediatorId(_) =>
            RecordingConfig(testDirectory(TargetDirectory).path)
          }
        } else {
          // We don't record submissions from Mediators in rounds higher than 1 for simplicity.
          MediatorNodeBootstrap.replaySequencerConfig.set(PartialFunction.empty)
        }
      }

      nodes.local.start()

      if (enableRecording) {
        participants.local.foreach(
          _.underlying.value.recordSequencerInteractions
            .set(Some(RecordingConfig(testDirectory(TargetDirectory).path)))
        )
      }

      participants.all.synchronizers.reconnect_all()

      runUntilCompletion(runners)

      nodes.local.stop()
  }
}

object PerformanceRunnerRecorder {

  private def monitoringThroughput[A](
      participants: Seq[ParticipantReference],
      reportEveryNumTx: Int,
      logger: TracedLogger,
      onReport: String => Unit,
      onTransaction: Transaction => Unit,
  )(within: => A): A = {
    val closeParticipants = participants.map { participant =>
      val numberOfTransactions = new AtomicInteger(0)
      val startTime = new AtomicReference(Instant.now())
      val observer =
        new ErrorLoggingStreamObserver[UpdateWrapper](
          logger,
          participant.name,
          "transactions.subscribe",
        )(TraceContext.empty) {
          override def onNext(value: UpdateWrapper): Unit = value match {
            case TransactionWrapper(transaction) =>
              onTransaction(transaction)
              if (numberOfTransactions.incrementAndGet() % reportEveryNumTx == 0) {
                val now = Instant.now()
                val duration = JDuration.between(startTime.getAndSet(now), now)
                val rate = reportEveryNumTx * 1000 / duration.toMillis
                onReport(
                  s"Throughput at $participant is $rate tx/s after ${numberOfTransactions.get()} tx."
                )
              }
            case _ =>
          }
        }

      val allParties = participant.parties.list().map(_.party)
      val queryAllPartiesFormat = UpdateFormat(
        includeTransactions = Some(
          TransactionFormat(
            eventFormat = Some(
              EventFormat(
                filtersByParty = allParties.map(_.toLf -> Filters(Nil)).toMap,
                filtersForAnyParty = None,
                verbose = false,
              )
            ),
            transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
          )
        ),
        includeReassignments = None,
        includeTopologyEvents = None,
      )

      participant.ledger_api.updates.subscribe_updates(
        observer,
        queryAllPartiesFormat,
        beginOffsetExclusive = participant.ledger_api.state.end(),
      )
    }
    ResourceUtil.withResource((() => closeParticipants.foreach(_.close)): AutoCloseable)(_ =>
      within
    )
  }
}

class BftSequencerPerformanceRunnerRecorder(
    override val startingParticipantIndex: Int = 1,
    override val numberOfParticipantsPerGroup: Option[Int] = None,
) extends PerformanceRunnerRecorder {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      shouldOverwriteStoredEndpoints = true,
      shouldBenchmarkBftSequencer = true,
    )
  )
}

class ReferenceSequencerPerformanceRunnerRecorder extends PerformanceRunnerRecorder {
  override protected val referenceBlockSequencerPlugin: Option[UseReferenceBlockSequencer[?]] =
    Some(
      new UseReferenceBlockSequencer[DbConfig.Postgres](
        loggerFactory,
        postgres = Some(postgresPlugin),
      )
    )

  referenceBlockSequencerPlugin.foreach(registerPlugin(_))
}
