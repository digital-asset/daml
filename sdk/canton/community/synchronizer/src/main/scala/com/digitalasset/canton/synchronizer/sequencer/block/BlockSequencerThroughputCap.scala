// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import com.daml.metrics.api.MetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.SubmissionRequestType
import com.digitalasset.canton.synchronizer.block.update.{ChunkUpdate, OrderedBlockUpdate}
import com.digitalasset.canton.synchronizer.metrics.{SequencerMetrics, ThroughputCapMetrics}
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.{
  IndividualThroughputCapConfig,
  ThroughputCapConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.SubmissionOutcome
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerThroughputCap.{
  IndividualBlockSequencerThroughputCap,
  SubmissionRequestEntry,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Mutex
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.actor.{Cancellable, Scheduler}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext

/** Throughput cap that functions to protect the overall availability of the sequencer network. This
  * algorithm provides a flexible and fair cap computation for all active sequencer clients (e.g.,
  * validators in the global synchronizer).
  *
  * Requirements:
  *   - Hard Cap: load on the network does not exceed the pre-configured maximum
  *   - Modifiable: cap settings can be modified without a system restart (TODO(i28703))
  *   - Fairness: under load, throughput is allocated fairly to each client
  *   - Preference Settings: some clients may get higher throughput allocation via sequencer
  *     operator configuration (TODO(i28703))
  *   - Elastic Demand Distribution: clients can exceed their fair share of throughput if the system
  *     provides sufficient capacity
  *   - Quality of Service: clients may designate their transactions with a quality of service label
  *     to indicate the importance of each one (TODO(i28703))
  *   - Message-type aware: caps are implemented with message type in mind to mainly target
  *     voluntary messages (e.g., confirmation and topology requests)
  *
  * Non-requirements:
  *   - No Resilience to Malicious Sequencers
  *   - Approximation
  *   - No Storage
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class BlockSequencerThroughputCap(
    config: ThroughputCapConfig,
    clock: Clock,
    scheduler: Scheduler,
    metrics: SequencerMetrics,
    timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with AutoCloseable {

  private val lock = new Mutex()
  private val perMessageTypeCaps =
    Map[SubmissionRequestType, IndividualBlockSequencerThroughputCap](
      SubmissionRequestType.ConfirmationRequest -> makeIndividualCap(
        config.messages.confirmationRequest,
        SubmissionRequestType.ConfirmationRequest,
      ),
      SubmissionRequestType.TopologyTransaction -> makeIndividualCap(
        config.messages.topology,
        SubmissionRequestType.TopologyTransaction,
      ),
    )

  private def makeIndividualCap(
      individualConfig: IndividualThroughputCapConfig,
      requestType: SubmissionRequestType,
  ) =
    new IndividualBlockSequencerThroughputCap(
      config.observationPeriodSeconds,
      individualConfig,
      requestType,
      clock,
      metrics,
      timeouts,
      loggerFactory,
    )

  private val enabled: AtomicBoolean = new AtomicBoolean(config.enabled)
  private var cancellable: Option[Cancellable] = None

  def shouldRejectTransaction(
      requestType: SubmissionRequestType,
      member: Member,
      requestLevel: Int,
  ): Either[String, Unit] =
    if (!enabled.get()) Right(())
    else
      perMessageTypeCaps
        .get(requestType)
        .map(_.shouldRejectTransaction(member, requestLevel))
        .getOrElse(Right(()))

  def addBlockUpdate(
      update: OrderedBlockUpdate
  ): Unit = if (enabled.get()) {
    val submissions = update match {
      case chunkUpdate: ChunkUpdate =>
        chunkUpdate.submissionsOutcomes
          .collect { case deliver: SubmissionOutcome.Deliver =>
            deliver
          }
          .map { deliver =>
            SubmissionRequestEntry(
              deliver.submission.sender,
              deliver.submission.requestType,
              deliver.sequencingTime,
              deliver.submission.toByteString.size().toLong,
            )
          }
      case _ => Seq.empty
    }

    addBlockUpdateInternal(submissions)
  }

  @VisibleForTesting
  private[block] def addBlockUpdateInternal(
      submissions: Seq[SubmissionRequestEntry]
  ): Unit = if (enabled.get())(lock.exclusive {
    cancellable.foreach(_.cancel().discard)

    submissions.foreach { submission =>
      perMessageTypeCaps
        .get(submission.requestType)
        .foreach(
          _.addEvent(
            timestamp = submission.sequencingTime,
            member = submission.sender,
            bytes = submission.bytes,
          )
        )
    }

    advanceWindow()
  })

  private def advanceWindow(): Unit = (lock.exclusive {
    perMessageTypeCaps.values.foreach(_.advanceWindow())
    scheduleClockTick()
  })

  private def scheduleClockTick(): Unit =
    cancellable = Some(
      scheduler.scheduleOnce(
        config.clockTickInterval.asJava,
        () => {
          advanceWindow()
        },
      )
    )

  override def close(): Unit = perMessageTypeCaps.foreach(_._2.close())
}

object BlockSequencerThroughputCap {
  private[block] final case class SubmissionRequestEntry(
      sender: Member,
      requestType: SubmissionRequestType,
      sequencingTime: CantonTimestamp,
      bytes: Long,
  )

  private final case class ThroughputCapEntry(
      timestamp: CantonTimestamp,
      key: ThroughputCapKey,
      value: ThroughputCapValue,
  )
  private final case class ThroughputCapKey(member: Member) extends AnyVal
  private[block] final case class ThroughputCapValue(bytes: Long, count: Int)

  /** Helper class to avoid a tuple in our computation
    *
    * @param totalOfHead
    *   sum of traffic spent by the leading spenders in the computation
    * @param adjustedHead
    *   sum of traffic spent by the leading spenders if they spent with the same rate of user at
    *   index
    * @param useOfIdx
    *   the rate by user at index
    * @param index
    *   the user index
    */
  private final case class Accumulate(
      totalOfHead: Long,
      adjustedHead: Long,
      useOfIdx: Long,
      index: Int,
  )

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  class IndividualBlockSequencerThroughputCap(
      observationPeriodSeconds: Int,
      config: IndividualThroughputCapConfig,
      requestType: SubmissionRequestType,
      clock: Clock,
      parentMetrics: SequencerMetrics,
      override protected val timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
  ) extends NamedLogging
      with FlagCloseable {

    private var initialized: Boolean = false

    private val thresholds =
      config.thresholds.sorted.reverse.zipWithIndex
    private val maximumGlobalTransactionsPerObservationPeriod =
      config.globalTpsCap.value * observationPeriodSeconds.toDouble
    private val maximumGlobalBytesPerObservationPeriod =
      config.globalKbpsCap.value * 1024 * observationPeriodSeconds.toDouble
    private val allowedTransactionsForMember =
      config.perClientTpsCap.value * observationPeriodSeconds // N_max_i_tps
    private val allowedBytesForMember =
      config.perClientKbpsCap.value * 1024 * observationPeriodSeconds // N_max_i_bps

    private val advancingWindow = new AtomicBoolean(false)
    private val advancingWindowLast = new AtomicReference(CantonTimestamp.MinValue)
    private var localTimeOfLatestEvent: CantonTimestamp = clock.now
    private var currentThresholdLevel: Int = thresholds.size
    private var throttledCountForMember: Double = config.perClientTpsCap.value
    private var throttledBytesForMember: Double = config.perClientKbpsCap.value * 1024

    private val lock = new Mutex()
    private val capWindow = new mutable.ArrayDeque[ThroughputCapEntry](
      initialSize = (observationPeriodSeconds * config.globalTpsCap.value).toInt
    )
    private var totalWindowBytes: Long = 0L;

    private val memberUsage = new TrieMap[ThroughputCapKey, ThroughputCapValue]()

    private val metrics =
      new ThroughputCapMetrics(
        requestType.name,
        parentMetrics.prefix,
        parentMetrics.openTelemetryMetricsFactory,
      )

    def shouldRejectTransaction(member: Member, requestLevel: Int): Either[String, Unit] = {
      val key = ThroughputCapKey(member)

      if (!initialized) Right(())
      else
        for {
          _ <- aboveMaxRate(key, requestLevel)
          _ <- aboveThrottledRate(key)
        } yield ()
    }

    private def aboveMaxRate(key: ThroughputCapKey, requestLevel: Int): Either[String, Unit] = {
      val usageByMember = memberUsage.getOrElse(key, ThroughputCapValue(0, 0)) // N_i

      lazy val overTps = usageByMember.count > allowedTransactionsForMember
      lazy val overKbps = usageByMember.bytes > allowedBytesForMember
      lazy val overThresholdLevel = requestLevel > currentThresholdLevel

      def explain(criteria: String) =
        "You are experiencing backpressure because your validator is exceeding the rate limits for a single validator " +
          "as configured by the synchronizer operators. If you need more bandwidth, please reach out to the operators. " +
          "The limit enforced is: " + criteria

      val result = for {
        _ <- Either.cond(
          !overTps,
          (),
          explain(
            s"${usageByMember.count} transactions over the past $observationPeriodSeconds seconds is more than the allowed ${f"$allowedTransactionsForMember%.1f"} for the period"
          ),
        )
        _ <- Either.cond(
          !overKbps,
          (),
          explain(
            s"${usageByMember.bytes} bytes over the past $observationPeriodSeconds seconds is more than the allowed ${f"$allowedBytesForMember%.1f"} for the period"
          ),
        )
        _ <- Either.cond(
          !overThresholdLevel,
          (),
          explain(
            s"Request at level $requestLevel is higher than the current threshold $currentThresholdLevel"
          ),
        )
      } yield ()

      result.left.foreach { _ =>
        metrics.rejections.mark()(
          MetricsContext(
            "member" -> key.member.toProtoPrimitive,
            "rejection_type" -> "per_member",
          )
        )
      }

      result
    }

    // R_t = (R_max - R_A) / (1 + V_active) + B_i
    // Note that R_A and B_i do not exist yet, so they are excluded from below for now
    private def aboveThrottledRate(key: ThroughputCapKey): Either[String, Unit] =
      if (currentThresholdLevel > 0) Right(())
      else {
        val vActive = memberUsage.size

        val usageByMember = memberUsage.getOrElse(key, ThroughputCapValue(0, 0)) // N_i

        lazy val overThrottledTps = usageByMember.count > throttledCountForMember
        lazy val overThrottledKbps = usageByMember.bytes > throttledBytesForMember

        def explain(criteria: String, globalCap: Double, individualUse: Long) =
          "You are experiencing backpressure because the network is congested and exceeds the " +
            s"configured global limits on the sequencer. Therefore, the sequencer is " +
            s"allocating the same bandwidth of ${f"$globalCap%.1f"} $criteria over $observationPeriodSeconds seconds to all " +
            s"$vActive active validators until the global usage rate drops again below the enforcement level. " +
            s"Please wait a few seconds and retry, as your current rate is $individualUse $criteria over $observationPeriodSeconds seconds."

        val result = for {
          _ <- Either.cond(
            !overThrottledTps,
            (),
            explain("transactions", throttledCountForMember, usageByMember.count.toLong),
          )
          _ <- Either.cond(
            !overThrottledKbps,
            (),
            explain("bytes", throttledBytesForMember, usageByMember.bytes),
          )
        } yield ()

        result.left.foreach { _ =>
          metrics.rejections.mark()(
            MetricsContext(
              "member" -> key.member.toProtoPrimitive,
              "rejection_type" -> "global",
            )
          )
        }

        result
      }

    // assumes that transactions are added in order of CantonTimestamp
    def addEvent(
        timestamp: CantonTimestamp,
        member: Member,
        bytes: Long,
    ): Unit = {
      val key = ThroughputCapKey(member)
      val value = ThroughputCapValue(bytes, 1)
      lock.exclusive {
        capWindow.addOne(ThroughputCapEntry(timestamp, key, value))
        totalWindowBytes += bytes
      }
      localTimeOfLatestEvent = clock.uniqueTime()

      memberUsage
        .updateWith(key) {
          case Some(value) => Some(ThroughputCapValue(value.bytes + bytes, value.count + 1))
          case None => Some(ThroughputCapValue(bytes, 1))
        }
        .discard

      advanceWindow()
    }

    def advanceWindow(): Unit =
      // only advance if we are not concurrently computing an advancement right now
      if (advancingWindow.compareAndSet(false, true)) {
        val now = clock.uniqueTime()
        // if we are in non-strict mode or if an update is due, update the thresholds
        if (
          config.strict || (now.toMicros - advancingWindowLast.get.toMicros) > config.updateEveryMs.value * 1000
        ) {
          advancingWindowLast.set(now)
          lock.exclusive {
            capWindow.lastOption.foreach { tailEntry =>
              val tailTimestamp = tailEntry.timestamp
              val removed = capWindow.removeHeadWhile { entry =>
                // removeHead while [t_head < t_tail + system_time - prev(system_time) - T_O]
                entry.timestamp.compareTo(
                  tailTimestamp
                    .plus(now - localTimeOfLatestEvent)
                    .minusSeconds(observationPeriodSeconds.toLong)
                ) < 0
              }

              // Once the window advances and events are removed, the cap logic has been running
              // for at least config.observationPeriodSeconds, and caps can be enforced
              if (!initialized && removed.nonEmpty)
                initialized = true

              // Update bookkeeping for events that no longer fall within the observation window
              removed.foreach { entry =>
                totalWindowBytes = math.max(0, totalWindowBytes - entry.value.bytes)
                memberUsage
                  .updateWith(entry.key) {
                    case Some(value) =>
                      val remainingBytes = value.bytes - entry.value.bytes
                      val remainingCount = value.count - entry.value.count
                      if (remainingBytes <= 0 && remainingCount <= 0)
                        None
                      else
                        Some(ThroughputCapValue(remainingBytes, remainingCount))

                    case None =>
                      noTracingLogger.warn(
                        s"Unexpected TPS Cap behavior: removed event from the capWindow for " +
                          s"member ${entry.key.member}, but member's usage map was empty."
                      )
                      None
                  }
                  .discard
              }
            }
          }

          calculateAndSetThresholdLevel()
          val (mode, newThrottledCountForMember, newThrottledBytesForMember) = if (config.strict) {
            // in strict mode, we give every member the same share of the rate
            val vActive = memberUsage.size
            (
              "strict",
              maximumGlobalTransactionsPerObservationPeriod / (1 + vActive.toDouble),
              maximumGlobalBytesPerObservationPeriod / (1 + vActive.toDouble),
            )
          } else {
            val (countTps, countBytes) = lock.exclusive {
              (capWindow.size, totalWindowBytes)
            }
            val newThrottledCountForMember = computePerMemberCap(
              currentTotalUse = countTps.toLong,
              maxGlobalCap = maximumGlobalTransactionsPerObservationPeriod,
              defaultPerMemberCap = allowedTransactionsForMember,
              get = _.count.toLong,
            )
            val newThrottledBytesForMember = computePerMemberCap(
              currentTotalUse = countBytes,
              maxGlobalCap = maximumGlobalBytesPerObservationPeriod,
              defaultPerMemberCap = allowedBytesForMember,
              get = _.bytes,
            )
            ("lenient", newThrottledCountForMember, newThrottledBytesForMember)
          }
          if (
            Math.abs(newThrottledCountForMember - throttledCountForMember) > 1.0e-6 ||
            Math.abs(newThrottledBytesForMember - throttledBytesForMember) > 1.0e-6
          ) {
            throttledCountForMember = newThrottledCountForMember
            throttledBytesForMember = newThrottledBytesForMember
            noTracingLogger.debug(
              s"Updated $mode per-member caps to count=$throttledCountForMember, bytes=$throttledBytesForMember"
            )
          }
          // Update the metrics unless we are closing
          synchronizeWithClosingSync(functionFullName) {
            metrics.tps.updateValue(capWindow.size.toDouble / observationPeriodSeconds.toDouble)
            metrics.bps.updateValue(totalWindowBytes.toDouble / observationPeriodSeconds.toDouble)
            metrics.tpsCap.updateValue(throttledCountForMember / observationPeriodSeconds.toDouble)
            metrics.bpsCap.updateValue(throttledBytesForMember / observationPeriodSeconds.toDouble)
          }(TraceContext.empty).discard
        }
        advancingWindow.set(false)
      }

    private def calculateAndSetThresholdLevel(): Unit = {
      val percentGlobalUtilizationTps =
        capWindow.size.toDouble / maximumGlobalTransactionsPerObservationPeriod
      val percentGlobalUtilizationBps =
        totalWindowBytes.toDouble / maximumGlobalBytesPerObservationPeriod
      val highestGlobalUtilization =
        math.max(percentGlobalUtilizationTps, percentGlobalUtilizationBps)

      val newLevel = thresholds
        .find { case (threshold, _) => highestGlobalUtilization >= threshold.value }
        .map { case (_, level) => level }
        .getOrElse(thresholds.size)
      if (newLevel != currentThresholdLevel) {
        noTracingLogger.info(
          s"Updating ${requestType.name} usage threshold from $currentThresholdLevel to $newLevel based on current use $highestGlobalUtilization"
        )
        currentThresholdLevel = newLevel
      }
    }

    private def computePerMemberCap(
        currentTotalUse: Long,
        maxGlobalCap: Double,
        defaultPerMemberCap: Double,
        get: ThroughputCapValue => Long,
    ) = {
      val sortedDescUsage = memberUsage.map { case (_, v) => v }.toSeq.sortBy(-get(_))
      val result =
        sortedDescUsage.zipWithIndex // assume we can iterate through the sorted list of users
          .scanLeft(Accumulate(0, currentTotalUse, 0, -1)) { case (acc, (use, idx)) =>
            val idxUse = get(use)
            // this would be the total bandwidth spent by the heavy users if they all would
            // submit at the rate of the user at position idx
            val adjusted = currentTotalUse - acc.totalOfHead - idxUse + (idxUse * (idx + 1))
            Accumulate(
              // this is the total bandwidth spent by the heavy users so far
              totalOfHead = acc.totalOfHead + idxUse,
              adjustedHead = adjusted,
              // this is the rate of spend at position idx
              useOfIdx = idxUse,
              // this is the index of the user so we count how many users make up totalOfHead
              index = idx,
            )
          }
          // now we scan through the list until the total use at the cap falls below the threshold
          .takeWhile { acc =>
            acc.adjustedHead > maxGlobalCap
          }
          .lastOption
      // result is now going to have the last index for which we exceed the throughput
      // we can now compute the perMemberCap for the high spenders to equally share
      // the excess bandwidth among each other
      result match {
        case Some(acc) =>
          (maxGlobalCap - (currentTotalUse - acc.totalOfHead)) / (acc.index + 1)
        case None =>
          defaultPerMemberCap
      }

    }

    @VisibleForTesting
    private[block] def getMemberUsage(member: Member): Option[ThroughputCapValue] =
      memberUsage.get(ThroughputCapKey(member))

    override def onClosed(): Unit = LifeCycle.close(metrics)(logger)

  }

}
