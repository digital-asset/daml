// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import cats.syntax.foldable.*
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{InstanceReference, LocalInstanceReference}
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, NamedLogging}
import com.digitalasset.canton.performance.elements.DriverStatus
import com.digitalasset.canton.performance.{PerformanceRunner, RateSettings}
import com.digitalasset.canton.topology.TopologyManagerError.SerialMismatch
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllButNamespaceDelegations
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.transaction.{
  MediatorSynchronizerState,
  SequencerSynchronizerState,
  SynchronizerTrustCertificate,
  TopologyChangeOp,
}
import com.digitalasset.canton.{TestEssentials, config}
import monocle.macros.syntax.lens.*
import org.apache.pekko.actor.ActorSystem
import org.scalatest.Assertion
import org.scalatest.time.{Millis, Minutes, Span}
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.util.Random
import scala.util.chaining.*

/** Base class for "topology chaos" tests. The test automatically adds an additional participant
  * configuration and renames it to "replay-config", for testing the replayability of the topology
  * history at the end of the test run.
  */
abstract class ChangingTopologyPerformanceIntegrationTest extends BasePerformanceIntegrationTest {
  setupPlugins(new UsePostgres(loggerFactory))

  import BasePerformanceIntegrationTest.*

  private val performanceRunnerTargetLatencyMs: Int = 1500

  // Buffer that we add on top of the target latency to compute the timeouts
  private val computationMarginMs: Int = 6000

  // Shorten the interval, as the default is 1 minute
  private val sequencerClientAcknowledgementIntervalMs: Int = 5000

  private val rateSettings = RateSettings(targetLatencyMs = performanceRunnerTargetLatencyMs)
  private val acceptableNumberOfFailedProgressChecks = 60

  private var performanceTestStart: CantonTimestamp = _
  private val totalChaosTime = config.NonNegativeFiniteDuration.ofSeconds(150)

  private val shouldStopRunTest = new AtomicReference(false)

  protected lazy val actorSystem = ActorSystem()
  protected lazy val scheduler = actorSystem.scheduler

  protected def numMediators: Int
  protected def numSequencers: Int
  protected def numParticipants: Int

  protected def operations: NonEmpty[Seq[TopologyOperations]]

  override protected lazy val baseEnvironmentConfig: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(numParticipants + 1, numSequencers, numMediators)
      .addConfigTransform(
        _.focus(_.participants).modify(cfg =>
          // replace the last "extra" participant with replay-config
          cfg + (InstanceName.tryCreate("replay-config") -> cfg(
            InstanceName.tryCreate(s"participant${numParticipants + 1}")
          )) -
            InstanceName.tryCreate(s"participant${numParticipants + 1}")
        )
      )
      .withManualStart
      .addConfigTransforms(
        _.focus(_.parameters.timeouts.processing.shutdownProcessing)
          .replace(config.NonNegativeDuration.tryFromDuration(1.minute)),
        _.focus(_.parameters.timeouts.console.bounded)
          .replace(config.NonNegativeDuration.tryFromDuration(5.minutes)),
        ConfigTransforms.updateSequencerClientAcknowledgementInterval(
          config.NonNegativeDuration
            .ofMillis(sequencerClientAcknowledgementIntervalMs.toLong)
            .toInternal
        ),
        // Disable warnings about consistency checks as this test creates a lot of contracts
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.activationFrequencyForWarnAboutConsistencyChecks)
            .replace(Long.MaxValue)
        ),
      )
      .addConfigTransforms(operations.forgetNE.flatMap(_.additionalConfigTransforms)*)
      .withSetup { implicit env =>
        import env.*
        sequencer1.start()
        mediator1.start()
        participant1.start()
        participant2.start()

        Seq[LocalInstanceReference](sequencer1, mediator1).foreach(
          _.health.wait_for_ready_for_initialization()
        )
        // Participants are fully initialized already when starting up (and auto init is true)
        Seq(participant1, participant2).foreach(
          _.health.wait_for_identity()
        )

        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
            synchronizerOwners = Seq(participant1),
            synchronizerThreshold = PositiveInt.one,
          )
        ).bootstrap()

        participant1.synchronizers.connect(sequencer1, daName)
        participant2.synchronizers.connect(sequencer1, daName)

        // Change the decision timeout so that we don't need to wait too long before changing party permission
        // from observation to confirmation after onboarding. Note that each value needs to be at least the
        // targetLatencyMs of the performance runner (+ some margin)
        synchronizerOwners1.foreach(
          _.topology.synchronizer_parameters
            .propose_update(
              daId,
              _.update(
                confirmationResponseTimeout = config.NonNegativeFiniteDuration
                  .ofMillis(performanceRunnerTargetLatencyMs.toLong + computationMarginMs),
                mediatorReactionTimeout = config.NonNegativeFiniteDuration
                  .ofMillis(performanceRunnerTargetLatencyMs.toLong + computationMarginMs),
              ),
              // when there are multiple synchronizer owners, waiting for the update to become effective would only work
              // after the nth synchronizer owner submitted the proposal that would fulfill the synchronizer owner quorum
              synchronize = None,
            )
        )

        operations.foreach(_.additionalSetupPhase())
      }

  private final class WaitGroup(labelsToWaitFor: Set[String]) {
    private val reference = new AtomicReference(labelsToWaitFor)
    private val promise = Promise[Unit]()

    def done(label: String): Unit = {
      val currentlyWaitingFor = reference.updateAndGet { oldSet =>
        val newSet = oldSet - label
        if (newSet.sizeCompare(oldSet) >= 0) {
          logger.error(s"Unknown key $label we are waiting for $oldSet")
        }
        newSet
      }
      logger.info(s"WaitGroup remaining: $currentlyWaitingFor")
      if (currentlyWaitingFor.isEmpty) {
        promise.success(())
      }
    }

    def waitF(): Future[Unit] = promise.future

    def waitingForCurrently(): Set[String] = reference.get()
  }

  // Be extra patient for the chaos test.
  override implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(20, Minutes)), interval = scaled(Span(500, Millis)))

  "preliminary checks" in { () =>
    def ensureNodeExclusivityUnique(proj: Reservations => Seq[String], nodeType: String) = {

      // node name -> classes claiming exclusive usage of the node
      val exclusiveNodes: Seq[(String, Seq[String])] = operations.forgetNE
        .flatMap(op => proj(op.reservations).map((op.getClass.getSimpleName, _)))
        .groupBy { case (_, node) => node }
        .view
        .mapValues(_.map { case (opName, _) => opName })
        .toList

      val conflicts = exclusiveNodes.filter { case (_, opNames) => opNames.sizeCompare(1) > 0 }

      clue(s"at most one class can claim exclusive usage of $nodeType") {
        conflicts shouldBe empty
      }
    }

    ensureNodeExclusivityUnique(_.exclusiveParticipants, "participants")
    ensureNodeExclusivityUnique(_.exclusiveMediators, "mediators")
    ensureNodeExclusivityUnique(_.exclusiveSequencers, "sequencers")
  }

  private implicit val globalReservations: Reservations = operations.forgetNE
    .foldLeft(Reservations())((acc, op) => acc + op.reservations)

  "initialization" in { implicit env =>
    import env.*

    participant1.health.ping(participant2)
    operations.foreach(_.initialization())
  }

  "run a normal performance test with ongoing topology changes" in { implicit env =>
    import env.*

    val (p1Config, p2Config) =
      defaultConfigs(
        0,
        participant1,
        participant2,
        withPartyGrowth = 0, // for now, don't add parties for additional topology activity
        totalCycles =
          1000000000, // run forever since we deactivate the workload after topology chaos is done
        rateSettings = rateSettings,
      )

    val runnerP1 =
      new PerformanceRunner(
        p1Config,
        _ => NoOpMetricsFactory,
        loggerFactory.append("participant", "participant1"),
      )
    val runnerP2 =
      new PerformanceRunner(
        p2Config,
        _ => NoOpMetricsFactory,
        loggerFactory.append("participant", "participant2"),
      )

    val runners = Seq(runnerP1, runnerP2)

    runners.foreach(env.environment.addUserCloseable(_))

    /*
      Because of the high load, some of the warnings (e.g., a participant processing a timeout) are emitted
      quite late. Therefore, the scope of the suppressing logger is extended to be as big as possible
     */
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        runners.foreach(_.startup().discard)

        performanceTestStart = environment.clock.now

        // wait until proposals progressed a bit
        eventually(timeUntilSuccess = 120.seconds) {
          proposalsAndAcceptsThusFar(runnerP1)._1 should be > 10
        }
        val waitGroup = new WaitGroup(
          (operations.map(_.name) ++ runners.zipWithIndex.map(x => s"runner${x._2}")).toSet
        )

        operations.forgetNE.foreach(scheduleOperation(waitGroup))
        runners.zipWithIndex.foreach { case (runner, index) =>
          scheduleProgressCheck(waitGroup)(s"runner$index", runner)
        }

        waitTimeout.await(
          s"Wait for chaos to finish, still waiting for ${waitGroup.waitingForCurrently()}"
        )(waitGroup.waitF())

        operations.foreach(_.finalAssertions())

        runners.foreach(_.setActive(false))

        Threading.sleep(5000)

        runners.foreach(_.close())

        // Before leaving the suppressing logger scope, let the workload finish or time out, so that we
        // don't flake on expected warnings.
        waitUntilWorkloadSubmissionsProcessed()
        // Synchronize the participants so that they are all caught up by making them ping each others
        clue("[chaos testing] waiting until a final set of pings goes through") {
          val allOnboardedParticipants = sequencer1.topology.synchronizer_trust_certificates
            .list(daId)
            .map(_.item.participantId.identifier.unwrap)
            .distinct
            .map(p)

          eventually(timeUntilSuccess = waitTimeout.asFiniteApproximation) {
            allOnboardedParticipants.zip(allOnboardedParticipants.view.drop(1)).foreach {
              case (from, to) =>
                from.health.maybe_ping(to) shouldBe defined
            }
          }
        }

        // Run one last topology transaction through the synchronizer and wait until all synchronizer members have observed
        // that transaction to help ensure that no synchronizer member is behind consuming topology changes.
        val sequencedTimeOfDummyTransaction =
          waitUntilDummySynchronizerTopologyTransactionsProcessed()

        // Wait for the duration of a client sequencer acknowledgment interval (+margin) to ensure
        // that all acknowledgements have been processed by the sequencers.
        Threading.sleep((sequencerClientAcknowledgementIntervalMs + computationMarginMs).toLong)

        validateTopologyState(sequencedTimeOfDummyTransaction)
      },
      forEvery(_)(acceptableLogMessageIncludingTopologyChangeWarnings),
    )
  }

  private def waitUntilWorkloadSubmissionsProcessed()(implicit
      env: TestConsoleEnvironment
  ): Unit = clue("[chaos testing] wait until submitted transaction are fully processed") {
    import env.*
    val decisionTimeout =
      sequencer1.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(daId)
        .decisionTimeout
    Threading.sleep((decisionTimeout + computationMarginMs.milli).underlying.toMillis)
  }

  private def waitUntilDummySynchronizerTopologyTransactionsProcessed()(implicit
      env: TestConsoleEnvironment
  ): CantonTimestamp = clue("[chaos testing] dummy topology transaction") {
    import env.*
    val signingKey =
      clue(s"[chaos testing] propose dummy NSD transaction for sequencer1") {
        val identifierKey = sequencer1.keys.secret
          .generate_signing_key(usage = SigningKeyUsage.NamespaceOnly)
        sequencer1.topology.namespace_delegations.propose_delegation(
          sequencer1.namespace,
          identifierKey,
          CanSignAllButNamespaceDelegations,
          store = env.daId,
        )
        identifierKey
      }

    eventually(
      TopologyOperations.topologyChangeTimeout.asFiniteApproximation,
      retryOnTestFailuresOnly = false,
    ) {
      val synchronizerMembers = sequencer1.topology.transactions
        .list(
          store = daId,
          operation = Some(TopologyChangeOp.Replace),
          filterMappings = Seq(
            Code.SynchronizerTrustCertificate,
            Code.SequencerSynchronizerState,
            Code.MediatorSynchronizerState,
          ),
        )
        .result
        .map(_.mapping)
        .flatMap[InstanceReference] {
          case dtc: SynchronizerTrustCertificate => Seq(p(dtc.participantId.identifier.unwrap))
          case sds: SequencerSynchronizerState =>
            sds.active.map(sid => s(sid.identifier.unwrap)).forgetNE
          case mds: MediatorSynchronizerState =>
            mds.active.map(mid => m(mid.identifier.unwrap)).forgetNE
          case _ => Seq.empty
        }
        .distinct

      val membersString = synchronizerMembers.map(_.name).mkString(",")
      logger.info(
        s"[chaos testing] waiting for synchronizer members $membersString to observe the dummy transaction"
      )
      val sequencedTime = synchronizerMembers
        .map { member =>
          logger.info(s"Checking dummy transaction for $member")
          def fetchNSD = member.topology.namespace_delegations
            .list(
              store = env.daId,
              filterNamespace = sequencer1.namespace.filterString,
              filterTargetKey = Some(signingKey.fingerprint),
            )
          utils.retry_until_true(fetchNSD.nonEmpty)
          fetchNSD.loneElement.context.sequenced
        }
        .headOption
        .value
      logger.info(
        s"[chaos testing] Found the dummy transaction at $sequencedTime on synchronizer members $membersString"
      )
      CantonTimestamp.assertFromInstant(sequencedTime)
    }

  }

  private def validateTopologyState(
      timestampForTopologyChecks: CantonTimestamp
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    logger.info(s"Starting topology validations at timestamp $timestampForTopologyChecks")

    val allOnboardedMediators = sequencer1.topology.mediators
      .list(daId, timeQuery = TimeQuery.Snapshot(timestampForTopologyChecks))
      .flatMap(group => group.item.allMediatorsInGroup)
      .map(_.identifier.unwrap)
      .distinct
      .map(m)

    val allOnboardedSequencers =
      sequencer1.topology.sequencers
        .list(daId, timeQuery = TimeQuery.Snapshot(timestampForTopologyChecks))
        .loneElement
        .item
        .allSequencers
        .forgetNE
        .map(_.identifier.unwrap)
        .distinct
        .map(s)

    val allOnboardedParticipants =
      sequencer1.topology.synchronizer_trust_certificates
        .list(daId, timeQuery = TimeQuery.Snapshot(timestampForTopologyChecks))
        .map(_.item.participantId.identifier.unwrap)
        .distinct
        .map(p)

    val runningNodes =
      allOnboardedSequencers ++
        allOnboardedMediators ++
        allOnboardedParticipants

    val verification = new TopologyStateVerification(
      timestampForTopologyChecks,
      futureSupervisor,
      environment.clock,
      environment.config.parameters.timeouts.processing,
      environment.loggerFactory,
    )

    clue(s"validating topology state of ${runningNodes.map(_.name)}")(
      verification.ensureConsistentTopologyState(runningNodes, daId)
    )

    clue(s"validating sequencer snapshots of ${allOnboardedSequencers.map(_.name)}")(
      verification.ensureConsistentSequencerSnapshots(allOnboardedSequencers)
    )

    clue(s"validating replayability of topology transactions")(
      verification.ensureTopologyHistoryCanBeReplayed(
        sequencer1,
        unusedNode = lp("replay-config"),
        daId,
        staticSynchronizerParameters1,
      )
    )
  }

  private def proposalsAndAcceptsThusFar(runner: PerformanceRunner): (Int, Int) = runner
    .status()
    .collect { case traderStatus: DriverStatus.TraderStatus =>
      (traderStatus.proposals.observed, traderStatus.accepts.observed)
    }
    .combineAll

  private def scheduleProgressCheck(
      waitGroup: WaitGroup,
      oldNumberOfProposals: Int = -1,
      oldNumberOfAccepts: Int = -1,
      failedAttempts: Int = 0,
  )(
      runnerName: String,
      runner: PerformanceRunner,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val max = performanceTestStart.plus(totalChaosTime.asJava)
    val delay = config.NonNegativeFiniteDuration.ofSeconds(5)

    if (environment.clock.now >= max || shouldStopRunTest.get()) {
      logger.info("Chaos test time's up: progress check is done")
      waitGroup.done(runnerName)
    } else {
      val (newNumberOfProposals, newNumberOfAccepts) = proposalsAndAcceptsThusFar(runner)
      val newFailedAttempts =
        if (
          newNumberOfProposals <= oldNumberOfProposals && newNumberOfAccepts <= oldNumberOfAccepts
        ) {
          failedAttempts + 1
        } else {
          0
        }
      if (newFailedAttempts > acceptableNumberOfFailedProgressChecks) {
        logger.error(
          s"Chaos test $runnerName not making progress: Old proposals: $oldNumberOfProposals old accepts: $oldNumberOfAccepts New proposals: $newNumberOfProposals new accepts: $newNumberOfAccepts"
        )
        shouldStopRunTest.set(true)
      }
      scheduler.scheduleOnce(
        delay = delay.underlying,
        runnable = new Runnable {
          override def run(): Unit =
            scheduleProgressCheck(
              waitGroup,
              newNumberOfProposals,
              newNumberOfAccepts,
              newFailedAttempts,
            )(
              runnerName,
              runner,
            )
        },
      )
    }
  }

  private def scheduleOperation(waitGroup: WaitGroup)(
      operation: TopologyOperations
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val max = performanceTestStart.plus(totalChaosTime.asJava)
    val delay = config.NonNegativeFiniteDuration.ofMillis(2500)

    if (environment.clock.now >= max || shouldStopRunTest.get()) {
      operation.logOperationStep("time's up: runTopologyChanges")(s"completed ${operation.name}")
      waitGroup.done(operation.name)
    } else {
      operation
        .runTopologyChanges()
        .recover(throwable =>
          operation.logOperationStep(operation.name)(
            s"operation ${operation.name} threw an exception: $throwable",
            Level.WARN,
          )
        )
        .map { _ =>
          scheduler
            .scheduleOnce(
              delay = delay.underlying,
              runnable = new Runnable {
                def run(): Unit = scheduleOperation(waitGroup)(operation)
              },
            )
            .discard
        }
        .discard
    }
  }

  protected def acceptableLogMessageIncludingTopologyChangeWarnings(entry: LogEntry): Assertion = {
    val specificAdditional = operations.forgetNE.flatMap(_.companion.acceptableLogEntries)
    val specificAdditionalNonRetryable =
      operations.forgetNE.flatMap(_.companion.acceptableNonRetryableLogEntries)

    acceptableLogMessageExt(
      additional = Seq(
        SerialMismatch.id, // concurrent topology changes can result in stale serials,
        "No connection available", // new sequencer connection pool check
      ) ++ specificAdditional,
      additionalNonRetryable = specificAdditionalNonRetryable,
    )(entry)
  }
}

class ChangingTopologyPerformanceIntegrationMediatorTest
    extends ChangingTopologyPerformanceIntegrationTest {
  protected val numMediators = 10
  protected val numSequencers = 1
  protected val numParticipants = 2

  override lazy val operations: NonEmpty[Seq[TopologyOperations]] =
    NonEmpty.apply(Seq, new MediatorGroupChaos(Set.empty, logger))
}

class ChangingTopologyPerformanceIntegrationSequencerTest
    extends ChangingTopologyPerformanceIntegrationTest {
  protected val numMediators = 1
  protected val numSequencers = 10
  protected val numParticipants = 2

  override protected def operations: NonEmpty[Seq[TopologyOperations]] =
    NonEmpty.apply(Seq, new SequencerChaos(logger))
}

class ChangingTopologyPerformanceIntegrationPartyReplicationTest
    extends ChangingTopologyPerformanceIntegrationTest {
  protected val numMediators = 1
  protected val numSequencers = 1
  protected val numParticipants = 4

  override lazy val operations: NonEmpty[Seq[TopologyOperations]] =
    NonEmpty.apply(Seq, new PartyReplicationChaos(logger))
}

class ChangingTopologyPerformanceIntegrationRestartParticipantTest
    extends ChangingTopologyPerformanceIntegrationTest {
  protected val numMediators = 1
  protected val numSequencers = 1
  protected val numParticipants = 3

  override lazy val operations: NonEmpty[Seq[TopologyOperations]] =
    NonEmpty.apply(Seq, new RestartParticipantsChaos(logger))
}

class ChangingTopologyPerformanceIntegrationRestartSequencersTest
    extends ChangingTopologyPerformanceIntegrationTest {
  protected val numMediators = 1
  protected val numSequencers = 1
  protected val numParticipants = 3

  override lazy val operations: NonEmpty[Seq[TopologyOperations]] =
    NonEmpty.apply(Seq, new RestartSequencersChaos(logger))
}

class ChangingTopologyPerformanceIntegrationRestartMediatorsTest
    extends ChangingTopologyPerformanceIntegrationTest {
  protected val numMediators = 1
  protected val numSequencers = 1
  protected val numParticipants = 3

  override lazy val operations: NonEmpty[Seq[TopologyOperations]] =
    NonEmpty.apply(Seq, new RestartMediatorsChaos(logger))
}

class ChangingTopologyPerformanceIntegrationSynchronizerOwnerTest
    extends ChangingTopologyPerformanceIntegrationTest {
  protected val numMediators = 1
  protected val numSequencers = 1
  protected val numParticipants = 2

  override lazy val operations: NonEmpty[Seq[TopologyOperations]] =
    NonEmpty.apply(Seq, new SynchronizerOwnerChaos(2, logger))
}

class ChangingTopologyPerformanceIntegrationDecentralizedPartyTest
    extends ChangingTopologyPerformanceIntegrationTest {
  protected val numMediators = 1
  protected val numSequencers = 1
  protected val numParticipants = 2

  override lazy val operations: NonEmpty[Seq[TopologyOperations]] =
    NonEmpty.apply(Seq, new DecentralizedPartyChaos(2, "0Cleese", "participant2", logger))
}

class ChangingTopologyPerformanceIntegrationBalanceTopUpsTest
    extends ChangingTopologyPerformanceIntegrationTest {
  protected val numMediators = 1
  protected val numSequencers = 3
  protected val numParticipants = 6

  override lazy val operations: NonEmpty[Seq[TopologyOperations]] =
    NonEmpty.apply(Seq, new BalanceTopUpsChaos(logger))
}

class ChangingTopologyKeyRotationViaNamespaceDelegationTest
    extends ChangingTopologyPerformanceIntegrationTest {
  protected val numMediators = 1
  protected val numSequencers = 1
  protected val numParticipants = 3

  override lazy val operations: NonEmpty[Seq[TopologyOperations]] =
    NonEmpty.apply(
      Seq,
      new KeyRotationViaNamespaceDelegationChaos(logger),
    )
}

class ChangingTopologyKeyRotationOwnerToKeyTest extends ChangingTopologyPerformanceIntegrationTest {
  protected val numMediators = 1
  protected val numSequencers = 1
  protected val numParticipants = 3

  override lazy val operations: NonEmpty[Seq[TopologyOperations]] =
    NonEmpty.apply(
      Seq,
      new KeyRotationViaOwnerToKeyChaos(logger),
    )
}

// If you add or remove a "specific" chaos test, adjust the number of
// CI buckets in canton_nightly.yml:topology_chaos_test accordingly

/** Mixin trait for tests that combine chaos modules. Allows having multiple n-way chaos tests (for
  * different values of n)
  */
trait AllTopologyChaosOperations {
  this: TestEssentials & NamedLogging =>

  protected val numMediators = 10
  protected val numSequencers = 3
  protected val numParticipants = 6

  type OperationsBuilder = () => TopologyOperations

  // Builders allow choosing chaos modules without instantiating the others.
  protected def allBuilders(): NonEmpty[Seq[OperationsBuilder]] =
    NonEmpty.apply(
      Seq,
      () => new MediatorGroupChaos(Set("mediator1"), logger),
      () => new PartyReplicationChaos(logger),
      () => new SynchronizerOwnerChaos(1, logger),
      () => new BalanceTopUpsChaos(logger),
      () => new KeyRotationViaOwnerToKeyChaos(logger),
      () => new KeyRotationViaNamespaceDelegationChaos(logger),
      () => new SequencerChaos(logger),
      () => new DecentralizedPartyChaos(2, "0Cleese", "participant2", logger),
// Removed  -too many combination plus very impactful tests
//      () => new RestartParticipantsChaos(logger),
//      () => new RestartMediatorsChaos(logger),
//      () => new RestartSequencersChaos(logger),
    )

  protected def chooseOperations(howMany: PositiveInt): NonEmpty[Seq[TopologyOperations]] = NonEmpty
    .from(
      // Can be modify ad-hoc to get a specific combination of chaos modules.
      Random.shuffle(allBuilders()).take(howMany.value)
    )
    .getOrElse(throw new IllegalStateException("cannot end up with an empty list of operations"))
    .map(_())
    .tap { operations =>
      val msg =
        s"[chaos testing] Running with operations: ${operations.map(_.name).mkString(", ")}"
      logger.info(msg)
      // On CI, print to stdout so it's easier to quickly spot in UI which operations are being run
      if (sys.env.contains("CI")) {
        println(msg)
      }
    }
}

class ChangingTopologyPerformanceIntegrationAllOpsTest
    extends ChangingTopologyPerformanceIntegrationTest
    with AllTopologyChaosOperations {
  override lazy val operations: NonEmpty[Seq[TopologyOperations]] =
    chooseOperations(PositiveInt.tryCreate(3))
}
