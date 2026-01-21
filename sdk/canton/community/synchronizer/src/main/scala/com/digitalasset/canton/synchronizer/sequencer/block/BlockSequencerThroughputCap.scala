// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
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
import com.digitalasset.canton.util.Mutex
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.actor.{Cancellable, Scheduler}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

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
  final case class SubmissionRequestEntry(
      sender: Member,
      requestType: SubmissionRequestType,
      sequencingTime: CantonTimestamp,
      bytes: Long,
  )

  final case class ThroughputCapEntry(
      timestamp: CantonTimestamp,
      key: ThroughputCapKey,
      value: ThroughputCapValue,
  )
  final case class ThroughputCapKey(member: Member)
  final case class ThroughputCapValue(bytes: Long, count: Int)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  class IndividualBlockSequencerThroughputCap(
      observationPeriodSeconds: Int,
      config: IndividualThroughputCapConfig,
      requestType: SubmissionRequestType,
      clock: Clock,
      parentMetrics: SequencerMetrics,
      override val loggerFactory: NamedLoggerFactory,
  ) extends NamedLogging
      with AutoCloseable {

    private var initialized: Boolean = false

    private val maximumGlobalTransactionsPerObservationPeriod =
      config.globalTpsCap.value * observationPeriodSeconds.toDouble
    private val maximumGlobalBytesPerObservationPeriod =
      config.globalKbpsCap.value * 1024 * observationPeriodSeconds.toDouble

    private var localTimeOfLatestEvent: CantonTimestamp = clock.now
    private var currentThresholdLevel: Int = 3

    private val capWindow = new mutable.ArrayDeque[ThroughputCapEntry](
      initialSize = (observationPeriodSeconds * config.globalTpsCap.value).toInt
    )
    private var totalWindowBytes: Long = 0

    private val memberUsage = new ConcurrentHashMap[ThroughputCapKey, ThroughputCapValue]().asScala

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
      val allowedTransactionsForMember =
        config.perClientTpsCap.value * observationPeriodSeconds // N_max_i_tps
      val allowedBytesForMember =
        config.perClientKbpsCap.value * 1024 * observationPeriodSeconds // N_max_i_bps

      lazy val overTps = usageByMember.count > allowedTransactionsForMember
      lazy val overKbps = usageByMember.bytes > allowedBytesForMember
      lazy val overThresholdLevel = requestLevel > currentThresholdLevel

      val result = for {
        _ <- Either.cond(
          !overTps,
          (),
          s"${usageByMember.count} transactions over the past $observationPeriodSeconds seconds is more than the allowed $allowedTransactionsForMember for the period",
        )
        _ <- Either.cond(
          !overKbps,
          (),
          s"${usageByMember.bytes} bytes over the past $observationPeriodSeconds seconds is more than the allowed $allowedBytesForMember for the period",
        )
        _ <- Either.cond(
          !overThresholdLevel,
          (),
          s"Request at level $requestLevel is higher than the current threshold $currentThresholdLevel",
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
        val vActive = memberUsage.size.toDouble
        val throttledCountForMember = maximumGlobalTransactionsPerObservationPeriod / (1 + vActive)
        val throttledBytesForMember = maximumGlobalBytesPerObservationPeriod / (1 + vActive)
        val usageByMember = memberUsage.getOrElse(key, ThroughputCapValue(0, 0)) // N_i

        lazy val overThrottledTps = usageByMember.count > throttledCountForMember
        lazy val overThrottledKbps = usageByMember.bytes > throttledBytesForMember

        val result = for {
          _ <- Either.cond(
            !overThrottledTps,
            (),
            s"${usageByMember.count} transactions over the past $observationPeriodSeconds seconds is more than the allowed $throttledCountForMember throttled amount for the period",
          )
          _ <- Either.cond(
            !overThrottledKbps,
            (),
            s"${usageByMember.bytes} bytes over the past $observationPeriodSeconds seconds is more than the allowed $throttledBytesForMember throttled amount for the period",
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

      capWindow.addOne(ThroughputCapEntry(timestamp, key, value))
      totalWindowBytes += bytes
      memberUsage
        .updateWith(key) {
          case Some(value) => Some(ThroughputCapValue(value.bytes + bytes, value.count + 1))
          case None => Some(ThroughputCapValue(bytes, 1))
        }
        .discard

      localTimeOfLatestEvent = clock.uniqueTime()
      advanceWindow()
    }

    def advanceWindow(): Unit = {
      val now = clock.uniqueTime()
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
        initialized = initialized || removed.nonEmpty

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

      calculateAndSetThresholdLevel()

      metrics.tps.updateValue(capWindow.size.toDouble / observationPeriodSeconds.toDouble)
      metrics.bps.updateValue(totalWindowBytes.toDouble / observationPeriodSeconds.toDouble)
    }

    private def calculateAndSetThresholdLevel(): Unit = {
      val percentGlobalUtilizationTps =
        capWindow.size.toDouble / maximumGlobalTransactionsPerObservationPeriod
      val percentGlobalUtilizationBps =
        totalWindowBytes.toDouble / maximumGlobalBytesPerObservationPeriod
      val highestGlobalUtilization =
        math.max(percentGlobalUtilizationTps, percentGlobalUtilizationBps)

      // TODO(i28703): Make configurable
      currentThresholdLevel =
        if (highestGlobalUtilization < 0.7) 3
        else if (highestGlobalUtilization < 0.8) 2
        else if (highestGlobalUtilization < 0.9) 1
        else 0
    }

    @VisibleForTesting
    private[block] def getMemberUsage(member: Member): Option[ThroughputCapValue] =
      memberUsage.get(ThroughputCapKey(member))

    override def close(): Unit = metrics.close()
  }
}
