// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveInt,
}
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.manual.topology.TopologyOperations.TransactionProgress
import com.digitalasset.canton.integration.util.OnboardsNewSequencerNode
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.FutureUtil
import com.digitalasset.canton.{BaseTest, ScalaFuturesWithPatience, config}
import org.scalactic.source.Position
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/** Traffic control chaos test
  *
  * This test performs the following actions:
  *   - Onboard all sequencers and set a 2/3 threshold.
  *   - At every iteration:
  *     - Pick a subset of the running participants + mediators.
  *     - For each of them, pick a subset of the active sequencers and send them a top-up request.
  *     - If the subset of sequencers was more than the threshold, check that we eventually observe
  *       the serial of the top-up.
  */
class BalanceTopUpsChaos(override val logger: TracedLogger)
    extends TopologyOperations
    with ScalaFuturesWithPatience
    with OnboardsNewSequencerNode {
  import BalanceTopUpsChaos.*

  override def name: String = "BalanceTopUpsChaos"
  override def companion: TopologyOperationsCompanion = BalanceTopUpsChaos

  private var random: Random = _
  private val balanceUpdaters = new AtomicReference[Set[BalanceUpdater]](Set.empty)
  private val activeSequencers = new AtomicReference[ActiveSequencers]
  private val availableParticipants = new AtomicReference[Seq[LocalParticipantReference]](Seq.empty)
  private val startedParticipants = new AtomicReference[Seq[LocalParticipantReference]](Seq.empty)

  private val InitialTopUp = 5_000_000L
  private val MinimumTopUp = 1_000_000L
  private val MaximumTopUp = 2_000_000L

  private val pendingTasks = new PendingTasks

  override lazy val reservations: Reservations =
    Reservations(exclusiveParticipants = Seq("participant5", "participant6"))

  private class BalanceUpdater(val member: Member) extends Matchers {
    private val lastSerial = new AtomicReference[PositiveInt](PositiveInt.one)
    // Only used to compute a new balance
    private val lastBalance =
      new AtomicReference[NonNegativeLong](NonNegativeLong.zero)
    private val lastObservedBalance =
      new AtomicReference[Option[(PositiveInt, NonNegativeLong)]](None)

    override def hashCode(): Int = member.hashCode()

    override def equals(other: Any): Boolean = other match {
      case updater: BalanceUpdater => member == updater.member
      case _ => false
    }

    def sendTopUp(
        topUpValueO: Option[NonNegativeLong] = None,
        initialTopUp: Boolean = false,
    )(implicit
        loggingContext: ErrorLoggingContext,
        env: TestConsoleEnvironment,
    ): Future[Unit] = {
      import env.*

      val serial = lastSerial.getAndUpdate(_.increment)
      val topUpValue =
        topUpValueO.getOrElse(
          NonNegativeLong.tryCreate(MinimumTopUp + random.nextLong(MaximumTopUp - MinimumTopUp + 1))
        )
      val newBalance = lastBalance.updateAndGet(_ + topUpValue)

      val activeSeqs = activeSequencers.get
      val threshold = activeSeqs.threshold.value

      val subset =
        if (initialTopUp) activeSeqs.sequencers
        else
          // We want to allow the case of sending the top-up to too few sequencers so that
          // the threshold is not reached, but "not too often".
          // We therefore bias 90% towards a number of sequencers >= threshold by picking a
          // subset of size between (threshold - 1) and (threshold + 8).
          subsetOf(
            activeSeqs.sequencers,
            minSize = if (threshold > 1) threshold - 1 else threshold,
            maxSize = threshold + 8,
          )

      val fut = subset.forgetNE.parTraverse_ { sequencer =>
        val message =
          s"to ${sequencer.name} for $member with serial = $serial, balance = $newBalance"
        Future {
          if (!initialTopUp)
            Threading.sleep(Random.nextLong(3000))

          withOperation_("send top-up")(message) {
            import cats.syntax.either.*

            // If the command fails, it is likely due to either TRAFFIC_CONTROL_TOP_UP_SUBMISSION_FAILED (submission
            // timed out) or TRAFFIC_CONTROL_SERIAL_TOO_LOW (top-up already effective due to delay).
            // In either case, we can safely ignore.
            // We catch the CommandFailure exception here instead of having "[chaos testing][send top-up]" in the
            // acceptable log entries, which might be too broad.
            Either
              .catchOnly[CommandFailure] {
                sequencer.traffic_control
                  .set_traffic_balance(member, serial, newBalance)
              }
          }
        }
      }

      // Only check the serial for top-ups sent to >= threshold-many sequencers
      fut.map(_ =>
        if (subset.sizeCompare(activeSeqs.threshold.value) >= 0) checkBalanceSerial(serial)
      )
    }

    private def checkBalanceSerial(serial: PositiveInt)(implicit
        loggingContext: ErrorLoggingContext,
        env: TestConsoleEnvironment,
    ): Unit = {
      import env.*
      import org.scalatest.LoneElement.*

      val snapshot = activeSequencers.get

      val fut = Future {
        withOperation_("check balance serial")(s">= $serial for $member") {
          val (latestSerial, latestBalance) = BaseTest.eventually() {
            val (currentSerialO, currentBalance) = snapshot.sequencers.forgetNE
              .parTraverse { sequencer =>
                Future {
                  val (_, trafficState) = sequencer.traffic_control
                    .traffic_state_of_members(Seq(member))
                    .trafficStates
                    .head
                  (trafficState.serial, trafficState.extraTrafficPurchased)
                }
              }
              .futureValue
              .distinct
              .loneElement

            val currentSerial = currentSerialO.getOrElse(fail())
            currentSerial should be >= serial

            (currentSerial, currentBalance)
          }

          lastObservedBalance.updateAndGet {
            case None => Some((latestSerial, latestBalance))
            case Some((oldSerial, _)) if latestSerial > oldSerial =>
              Some((latestSerial, latestBalance))
            case old => old
          }.discard
        }
      }

      pendingTasks.addTask(s"observe serial >= $serial for $member", fut)
    }

    def checkFinalBalance()(implicit
        loggingContext: ErrorLoggingContext,
        env: TestConsoleEnvironment,
    ): Unit = {
      import env.*

      lastObservedBalance.get.foreach { case (_, expectedFinalBalance) =>
        withOperation_("check final balance")(s"$expectedFinalBalance for $member") {
          val trafficState = sequencer1.traffic_control
            .traffic_state_of_members(Seq(member))
            .trafficStates
            .head
            ._2

          val actualBalance = trafficState.extraTrafficPurchased

          actualBalance shouldBe expectedFinalBalance
        }
      }
    }
  }

  override def initialization()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Unit = {
    import env.*

    val seed = Random.nextLong()
    random = new Random(seed)
    logOperationStep("top-up chaos")(s"Starting Balance Top-Up Updates Chaos test with seed $seed")

    val startedParticipants = participants.local.filter(_.is_running).map(_.id: Member)
    val startedMediators = mediators.local.filter(_.is_running).map(_.id: Member)

    val updaters = (startedParticipants ++ startedMediators).map(new BalanceUpdater(_))

    // Give members an initial top-up so they can start talking
    updaters
      .parTraverse(updater =>
        updater.sendTopUp(Some(NonNegativeLong.tryCreate(InitialTopUp)), initialTopUp = true)
      )
      .futureValue

    balanceUpdaters.set(updaters.toSet)
  }

  override def finalAssertions(transactionProgress: TransactionProgress)(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Unit = {
    import env.*

    // Wait for the pending tasks to complete
    pendingTasks.future.futureValue

    // Ensure our extra participants are connected to the synchronizer so the final validation applies to them
    // Increase the patience as it seems the default 5 seconds might be too short.
    val extraPatience = PatienceConfig(timeout = Span(10, Seconds), interval = Span(20, Millis))
    reservations.exclusiveParticipants
      .map(lp)
      .filter(!_.health.is_running())
      .parTraverse_ { p =>
        logOperationStep("top-up finalization")(s"start and connect participant ${p.name}")
        Future {
          p.start()
          // connect without synchronizing the topology, otherwise we might try to access
          // the health endpoint of a not yet fully started
          p.synchronizers.connect(
            daName,
            sequencer1.sequencerConnection.endpoints.head1.toURI(false).toString,
            synchronize = None,
          )
        }
      }
      .futureValue(extraPatience, Position.here)

    // Check final balances
    balanceUpdaters.get.toSeq.parTraverse_ { updater =>
      Future(updater.checkFinalBalance())
    }.futureValue
  }

  private val trafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(20 * 1000L),
    readVsWriteScalingFactor = InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
    // Enough to bootstrap the synchronizer and connect the participant after 1 second
    maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
    setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
    enforceRateLimiting = true,
    baseEventCost = NonNegativeLong.tryCreate(100L),
  )

  override def additionalSetupPhase()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    // Start all extra local sequencers
    sequencers.local.filter(!_.is_running).foreach { localSequencer =>
      localSequencer.start()
      localSequencer.health.wait_for_running()
    }

    // We assume the remote sequencers have been started

    val synchronizerOwners = getSynchronizerOwners(daId, sequencer1)
    val (currentSequencers, _) = getSynchronizerSequencers(daId, sequencer1)

    // Onboard all extra sequencers
    NonEmpty.from(sequencers.all.filter(s => !currentSequencers.contains(s))) match {
      case None => // No extra sequencer to onboard
      case Some(sequencersToOnboard) =>
        sequencersToOnboard.foreach { sequencer =>
          onboardNewSequencer(
            daId,
            newSequencerReference = sequencer,
            existingSequencerReference = currentSequencers.head1,
            synchronizerOwners = synchronizerOwners,
          )
        }

        val allSequencers = currentSequencers ++ sequencersToOnboard

        // Make the threshold ceil(2/3 of the sequencers)
        val threshold = {
          val size = allSequencers.size
          val t = size * 2 / 3
          if (t * 3 / 2 < size) t + 1 else t
        }

        // Update the sequencer topology
        synchronizerOwners.forgetNE.toSeq.parTraverse { owner =>
          Future {
            owner.topology.sequencers.propose(
              synchronizerId = daId,
              threshold = PositiveInt.tryCreate(threshold),
              active = allSequencers.map(_.id),
              mustFullyAuthorize = true,
            )
          }
        }.futureValue
        utils.retry_until_true {
          sequencer1.topology.sequencers.list(daId).flatMap(_.item.active).toSet == allSequencers
            .map(_.id)
            .toSet
            .forgetNE
        }
    }

    availableParticipants.set(reservations.exclusiveParticipants.map(lp))

    val (newSynchronizerSequencers, newThreshold) = getSynchronizerSequencers(daId, sequencer1)
    activeSequencers.set(ActiveSequencers(newSynchronizerSequencers, newThreshold))

    synchronizerOwners.forgetNE.toSeq.parTraverse { owner =>
      Future {
        owner.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(trafficControl = Some(trafficControlParameters)),
        )
      }
    }.futureValue
  }

  override def runTopologyChanges()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Future[Unit] = {
    import env.*

    val updateBalancesActionO = Some(() => updateBalances())
    val startParticipantActionO =
      Option.when(availableParticipants.get.nonEmpty)(() => startParticipant())
    val stopParticipantActionO =
      Option.when(startedParticipants.get.nonEmpty)(() => stopParticipant())
    val pingActionO =
      Option.when(startedParticipants.get.nonEmpty)(() => pingParticipant())

    val possibleActions = NonEmpty
      .from(
        Seq(
          // Favor updating balances
          updateBalancesActionO,
          updateBalancesActionO,
          updateBalancesActionO,
          startParticipantActionO,
          stopParticipantActionO,
          pingActionO,
        ).flatten
      )
      .getOrElse(throw new IllegalStateException("No possible action"))

    subsetOf(possibleActions, 1, 1).head1.apply()
  }

  private def updateBalances()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
  ): Future[Unit] = {
    import env.*

    NonEmpty.from(balanceUpdaters.get) match {
      case Some(updatersNE) =>
        val subset = subsetOf(updatersNE.toSeq, 1, updatersNE.size)
        subset.forgetNE.parTraverse_(_.sendTopUp())
      case _ => Future.unit
    }
  }

  private def startParticipant()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
  ): Future[Unit] = {
    import env.*

    val candidates = availableParticipants.getAndUpdate(_.drop(1))

    Future {
      candidates.headOption.foreach { participantToStart =>
        withOperation_("start participant")(s"${participantToStart.name}") {
          participantToStart.start()
          participantToStart.synchronizers.connect(sequencer1, daName)

          balanceUpdaters
            .updateAndGet(current => current + new BalanceUpdater(participantToStart.id))
            .discard
          startedParticipants
            .updateAndGet(current => random.shuffle(participantToStart +: current))
            .discard
        }
      }
    }
  }

  private def stopParticipant()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
  ): Future[Unit] = {
    import env.*

    val candidates = startedParticipants.getAndUpdate(_.drop(1))

    Future {
      NonEmpty.from(candidates) match {
        case None => // No participant has been started
        case Some(candidatesNE) =>
          val participantToStop = candidatesNE.head1

          withOperation_("stop participant")(s"${participantToStop.name}") {

            participantToStop.synchronizers.disconnect(daName)
            participantToStop.stop()

            // Not removing the corresponding balance updater, so as to also exercise topping  up members
            // that are disconnected.
            availableParticipants
              .updateAndGet(current => random.shuffle(participantToStop +: current))
              .discard
          }
      }
    }
  }

  private def pingParticipant()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
  ): Future[Unit] = {
    import env.*

    val candidates = random.shuffle(startedParticipants.get.take(2))

    Future {
      candidates match {
        case Seq(p1, p2) =>
          withOperation_("ping")(s"${p1.name} -> ${p2.name}") {
            p1.health.ping(p2)
          }
        case Seq(p) =>
          withOperation_("ping")(s"${p.name} -> itself") {
            p.health.ping(p)
          }
        case _ =>
      }
    }
  }

  private def subsetOf[A](
      seq: NonEmpty[Seq[A]],
      minSize: PositiveInt,
      maxSize: PositiveInt,
  ): NonEmpty[Seq[A]] = {
    require(maxSize >= minSize, s"maxSize ($maxSize) must be >= minSize ($minSize)")

    val shuffledSeq = random.shuffle(seq)
    val size = minSize.value + random.nextInt(maxSize.value - minSize.value + 1)
    val subset = shuffledSeq.take(size)

    NonEmpty.from(subset).getOrElse(throw new IllegalStateException("Empty subset"))
  }
}

object BalanceTopUpsChaos extends TopologyOperationsCompanion {
  private final case class ActiveSequencers(
      sequencers: NonEmpty[Seq[SequencerReference]],
      threshold: PositiveInt,
  )

  private class PendingTasks {
    private val tasks =
      new AtomicReference[Future[Unit]](Future.successful(()))

    def addTask(name: String, task: Future[Unit])(implicit
        ec: ExecutionContext,
        loggingContext: ErrorLoggingContext,
    ): Unit = {
      val fut = tasks.updateAndGet(_.flatMap(_ => task))

      FutureUtil.doNotAwait(fut, s"Failed to complete pending tasks up to '$name'")
    }

    def future: Future[Unit] = tasks.get
  }

  override lazy val acceptableLogEntries: Seq[String] = Seq(
    // If a top-up is submitted close to the end of a submission window, we send two requests (one
    // for each window), and the first one will likely timeout quickly
    "Submission timed out after sequencing time",
    // With traffic limits enforced, some submissions might not go through due to insufficient credit
    // This is the error logged when the submission is rejected on the write path before sequencing (the most likely)
    "Submission was rejected because not traffic is available",
    // This is the error logged if the submission is reject after sequencing
    "SEQUENCER_NOT_ENOUGH_TRAFFIC_CREDIT",
    // If a sequencer is "late to the party" when submitting a top-up balance, it can happen that it
    // has already been processed by enough sequencers and is effective
    "TRAFFIC_CONTROL_SERIAL_TOO_LOW",
  )
}
