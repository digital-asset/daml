// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalInstanceReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.tests.manual.topology.LogicalSynchronizerUpgradeChaos.{
  Action,
  NoAction,
  PerformLSU,
  SynchronizerData,
  quietTime,
  topologyFreezeDuration,
}
import com.digitalasset.canton.integration.tests.manual.topology.TopologyOperations.{
  TransactionProgress,
  topologyChangeTimeout,
}
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.{
  ConfigTransform,
  ConfigTransforms,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, config}
import org.scalatest.OptionValues.*
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, blocking}
import scala.math.Ordering.Implicits.*

/** This class performs repeated LSUs. It assumes the synchronizer has only one sequencer
  * (sequencer1) and one mediator (mediator1).
  *
  * The PSId of the successor is constructed as follows:
  *   - pv = dev
  *   - serial=previous serial + 1
  *
  * In terms of nodes, each LSU has an index i starting at one used as follows:
  *   - Sequencer and mediator of the successor are sequencerN and mediatorN where N=i+1
  *
  * @param maxLSU
  *   Maximum number of LSU to perform. Must be smaller than (M+1) where M is the number of
  *   available sequencers and mediators.
  */
private[topology] class LogicalSynchronizerUpgradeChaos(
    maxLSU: PositiveInt,
    val logger: TracedLogger,
) extends TopologyOperations
    with LogicalUpgradeUtils
    with Matchers {
  override val name: String = LogicalSynchronizerUpgradeChaos.name

  override protected def testName: String = name

  /*
  Map from LSU index to synchronizer nodes.
  Is used to stop previous nodes to limit the memory footprint of the test
   */
  private lazy val usedSynchronizerNodes: TrieMap[Int, Set[LocalInstanceReference]] = TrieMap()

  private lazy val lsuTimes = new ListBuffer[CantonTimestamp]

  /** Value:
    *   - At init: current synchronizer
    *   - When an LSU is announced: next synchronizer.
    *
    * Note: is expected to be set by [[additionalSetupPhase]] before [[runTopologyChanges]] is
    * called
    */
  private val lastKnownSynchronizer: AtomicReference[Option[SynchronizerData]] =
    new AtomicReference(None)

  override def companion: TopologyOperationsCompanion = LogicalSynchronizerUpgradeChaos

  override def additionalSetupPhase()(implicit env: TestConsoleEnvironment): Unit = {
    val synchronizerData =
      SynchronizerData(
        usedSince = CantonTimestamp.MinValue,
        psid = env.daId,
        index = PositiveInt.one,
      )

    require(
      env.sequencers.local.sizeIs >= maxLSU.unwrap + 1,
      s"Expecting at least ${maxLSU.unwrap + 1} local sequencers but found ${env.sequencers.local.size}",
    )

    require(
      env.mediators.local.sizeIs >= maxLSU.unwrap + 1,
      s"Expecting at least ${maxLSU.unwrap + 1} local mediators but found ${env.mediators.local.size}",
    )

    usedSynchronizerNodes.put(1, Set(env.sequencer1, env.mediator1))

    lastKnownSynchronizer.set(Some(synchronizerData))
  }

  override def additionalConfigTransforms: Seq[ConfigTransform] =
    // Synchronizer nodes used for LSU should not have auto init
    ConfigTransforms.disableAutoInit(
      (2 to maxLSU.unwrap + 1).flatMap(i => Seq(s"sequencer$i", s"mediator$i")).toSet
    ) +:
      // pv=dev is used
      ConfigTransforms.enableAlphaVersionSupport

  // Decide what to do next
  private def nextAction()(implicit env: TestConsoleEnvironment): Action = {
    val now = env.environment.now

    blocking(this.synchronized {
      val currentSynchronizer = lastKnownSynchronizer.get().value

      val scheduleNextLSU =
        (now - currentSynchronizer.usedSince >= quietTime.asJava) // keep enough time between LSUd
          && currentSynchronizer.index.decrement < maxLSU.toNonNegative // number of LSUs is capped

      if (scheduleNextLSU) {
        val nextPSId = PhysicalSynchronizerId(
          currentSynchronizer.psid.logical,
          currentSynchronizer.psid.serial.increment.toNonNegative,
          ProtocolVersion.dev,
        )

        val upgradeTime = now.plus(topologyFreezeDuration)

        PerformLSU(
          currentPSId = currentSynchronizer.psid,
          nextSynchronizer = SynchronizerData(
            usedSince = upgradeTime,
            psid = nextPSId,
            index = currentSynchronizer.index.increment,
          ),
          upgradeTime = upgradeTime,
        )
      } else NoAction
    })
  }

  private def performLSU(
      currentPSId: PhysicalSynchronizerId,
      newSynchronizer: SynchronizerData,
      upgradeTime: CantonTimestamp,
  )(implicit
      env: TestConsoleEnvironment,
      errorLoggingContext: ErrorLoggingContext,
  ): Unit = {
    import env.*
    val lsuId = s"lsu ${newSynchronizer.psid.suffix}"

    val synchronizerOwner = env.participant1

    val nextIndex = newSynchronizer.index.unwrap
    val currentIndex = nextIndex - 1

    val currentSequencer = ls(s"sequencer$currentIndex")
    val currentMediator = lm(s"mediator$currentIndex")

    val newSequencer = ls(s"sequencer$nextIndex")
    val newMediator = lm(s"mediator$nextIndex")

    usedSynchronizerNodes.put(nextIndex, Set(newSequencer, newMediator))
    lsuTimes.append(upgradeTime)

    /*
    Stop old nodes.
    We don't try to stop nodes just after the LSU because the risk of stopping them too early is too high.
     */
    usedSynchronizerNodes.getOrElse(currentIndex - 1, Set.empty).foreach(_.stop())
    logger.info(
      s"[$lsuId] Stopped nodes: ${usedSynchronizerNodes.getOrElse(currentIndex - 1, Set.empty)}"
    )

    newSequencer.start()
    newMediator.start()

    logOperationStep(lsuId)(s"Announce LSU to $lsuId at $upgradeTime")
    lastKnownSynchronizer.set(Some(newSynchronizer))

    logger.info(s"[$lsuId] $synchronizerOwner propose the announcement")
    synchronizerOwner.topology.synchronizer_upgrade.announcement
      .propose(newSynchronizer.psid, upgradeTime)
      .discard

    // Wait until announcement is effective and processed on the sequencer
    BaseTest.eventually(
      topologyChangeTimeout.asFiniteApproximation,
      retryOnTestFailuresOnly = false,
    ) {
      currentSequencer.topology.synchronizer_upgrade.announcement
        .list()
        .find(_.item.successorSynchronizerId == newSynchronizer.psid)
        .value
    }

    logOperationStep(lsuId)("Exporting synchronizer nodes data")

    val exportDirectory = exportNodesData(
      SynchronizerNodes(Seq(currentSequencer), Seq(currentMediator)),
      successorPSId = newSynchronizer.psid,
    )

    logOperationStep(lsuId)("Migrate synchronizer nodes")

    /*
    The order is important:
    - start the sequencer
    - set the lower bound
    - start the mediator; download of the correct topology state from the mediator depends on the lower bound.
     */
    Seq[InstanceReference](newSequencer, newMediator).foreach { newNode =>
      logger.info(s"[$lsuId]: Migrate node $newNode")
      migrateNode(
        migratedNode = newNode,
        newStaticSynchronizerParameters = newSynchronizer.staticSynchronizerParameters,
        synchronizerId = currentPSId,
        newSequencers = Seq(newSequencer),
        exportDirectory = exportDirectory,
        sourceNodeNames =
          Map(newSequencer.name -> currentSequencer.name, newMediator.name -> currentMediator.name),
      )

      newNode match {
        case localSeq: LocalSequencerReference =>
          localSeq.underlying.value.sequencer
            .setSequencingTimeLowerBoundExclusive(Some(upgradeTime))

        case _ => ()
      }
    }

    logOperationStep(lsuId)(s"Announcing sequencer successor of $currentSequencer")
    currentSequencer.topology.synchronizer_upgrade.sequencer_successors
      .propose_successor(
        sequencerId = currentSequencer.id,
        endpoints = newSequencer.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
        synchronizerId = currentPSId,
      )
      .discard

    logger.info(s"[$lsuId] All operations scheduled")
  }

  override def runTopologyChanges()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Future[Unit] = {
    import env.*

    logger.info(
      s"Connectivity info: ${participants.local.filter(_.is_running).map(p => s"${p.name} -> ${p.synchronizers.list_connected().map(_.physicalSynchronizerId.suffix)}")}"
    )

    nextAction() match {
      case LogicalSynchronizerUpgradeChaos.NoAction => Future.unit
      case PerformLSU(currentPSId, nextSynchronizer, upgradeTime) =>
        Future(performLSU(currentPSId, nextSynchronizer, upgradeTime))
    }
  }

  @nowarn("msg=match may not be exhaustive")
  override def finalAssertions(transactionProgress: TransactionProgress)(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Unit = {
    val progress: Map[String, Seq[(CantonTimestamp, Int)]] = transactionProgress.progress
      .groupMap { case ((runnerName, _), _) => runnerName } { case ((_, ts), txCount) =>
        ts -> txCount
      }
      .view
      .mapValues(_.toVector.sortBy { case (ts, _) => ts })
      .toMap

    /*
     For each runner, for each LSU, get the highest number of accepts+proposals before the upgrade time
     Ensure there is progress between two LSUs
     */
    progress.foreach { case (runner, data) =>
      // This is quite inefficient but easy to read and the number of LSUs is expected to be small
      val res = lsuTimes
        .map { lsuTime =>
          data.collect { case (time, accepts) if time < lsuTime => accepts }.lastOption
        }
        .toList
        // forget about None
        .flatten

      logger.info(s"Computed progress for $runner: $res")

      withClue("Expecting at least 3 LSUs") {
        res.size should be > 2
      }

      res.sliding(2).foreach { case List(before, after) =>
        before should be < after // some activity should happen
      }
    }
  }
}

private[topology] object LogicalSynchronizerUpgradeChaos extends TopologyOperationsCompanion {

  val name: String = "LSUChaos"

  //  Time between one announcement and the next one.
  private val quietTime: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofSeconds(10)

  private val topologyFreezeDuration: Duration = Duration.ofSeconds(15)

  private sealed trait Action extends Product with Serializable
  private case object NoAction extends Action
  private final case class PerformLSU(
      currentPSId: PhysicalSynchronizerId,
      nextSynchronizer: SynchronizerData,
      upgradeTime: CantonTimestamp,
  ) extends Action

  final case class SynchronizerData(
      usedSince: CantonTimestamp,
      psid: PhysicalSynchronizerId,
      index: PositiveInt,
  ) {
    val staticSynchronizerParameters: StaticSynchronizerParameters =
      StaticSynchronizerParameters.defaultsWithoutKMS(
        psid.protocolVersion,
        psid.serial,
        topologyChangeDelay = config.NonNegativeFiniteDuration.Zero,
      )
  }

  override def acceptableLogEntries: Seq[String] = Seq(
    // If submission is done during the LSU
    "SUBMISSION_SYNCHRONIZER_NOT_READY"
  )

  override def acceptableNonRetryableLogEntries: Seq[String] = Seq(
    // If submission is done during the LSU
    "SUBMISSION_SYNCHRONIZER_NOT_READY"
  )
}
