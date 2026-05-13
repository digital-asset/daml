// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.javaapi.data.Party
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricName
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.performance.acs.ContractStore
import com.digitalasset.canton.performance.control.{LatencyMonitor, SubmissionRate}
import com.digitalasset.canton.performance.elements.DriverStatus.{StepStatus, TraderStatus}
import com.digitalasset.canton.performance.elements.dvp.TraderStats
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.model.java.orchestration.TestProbeData
import com.digitalasset.canton.performance.{ActivePartyRole, Connectivity, RateSettings}
import com.digitalasset.canton.topology.SynchronizerId
import org.apache.pekko.actor.ActorSystem

import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.Ordered.orderingToOrdered
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.*

abstract class ParticipantDriver(
    connectivity: Connectivity,
    partyLf: LfPartyId,
    masterLf: LfPartyId,
    role: ActivePartyRole,
    registerGenerator: Boolean,
    prefix: MetricName,
    metricsFactory: LabeledMetricsFactory,
    loggerFactory: NamedLoggerFactory,
    control: DriverControl,
    baseSynchronizerId: SynchronizerId,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends BaseDriver(
      connectivity,
      partyLf,
      masterLf,
      role.commandClientConfiguration,
      loggerFactory,
      control,
    ) {

  private val settings_ = new AtomicReference[RateSettings](role.settings)

  def settings: RateSettings = settings_.get()

  override def updateRateSettings(update: RateSettings => RateSettings): Unit = {
    val upd = settings_.updateAndGet(update(_))
    rate.updateSettings(upd.startRate, upd.targetLatencyMs, upd.adjustFactor)
  }

  def name: String = role.name

  protected val testResult =
    new ContractStore[
      M.orchestration.TestParticipant.Contract,
      M.orchestration.TestParticipant.ContractId,
      M.orchestration.TestParticipant,
      Unit,
    ](
      "test result",
      M.orchestration.TestParticipant.COMPANION,
      index = _ => (),
      filter = x => x.data.master == masterParty.getValue && x.data.party == party.getValue,
      loggerFactory,
    )

  protected val participantRequest = new ContractStore[
    M.orchestration.ParticipationRequest.Contract,
    M.orchestration.ParticipationRequest.ContractId,
    M.orchestration.ParticipationRequest,
    Unit,
  ](
    "participation request",
    M.orchestration.ParticipationRequest.COMPANION,
    index = _ => (),
    filter = x => x.data.master == masterParty.getValue,
    loggerFactory,
  )

  protected val generator = new ContractStore[
    M.generator.Generator.Contract,
    M.generator.Generator.ContractId,
    M.generator.Generator,
    Unit,
  ](
    "my generator",
    M.generator.Generator.COMPANION,
    index = _ => (),
    filter = _ => true,
    loggerFactory,
  )

  protected val rate =
    new SubmissionRate.I(
      role.settings.startRate,
      role.settings.targetLatencyMs,
      role.settings.adjustFactor,
      prefix,
      metricsFactory,
      loggerFactory,
      () => CantonTimestamp.now(),
    )
  private val monitor = new LatencyMonitor(rate)

  override def latencyMonitor: Option[LatencyMonitor] = Some(monitor)

  private val initExistingCompleted: AtomicBoolean = new AtomicBoolean(false)

  protected final def initExisting(): Unit =
    if (!initExistingCompleted.getAndSet(true)) {
      val ret = doInitExisting()
      if (!ret) {
        initExistingCompleted.set(false)
      }
    }

  protected def doInitExisting(): Boolean = true

  this.listeners.appendAll(Seq(testResult, participantRequest, generator))

  override def start(): Future[Either[String, Unit]] =
    (for {
      _ <- EitherT(super.start())
      _ <- registerIfNecessary()
    } yield ()).value

  protected def registerIfNecessary(): EitherT[Future, String, Unit] =
    if (testResult.one(()).isEmpty && participantRequest.one(()).isEmpty) {
      val reference = s"requesting to registering $party with master"
      val pr = mapCommand(
        submitCommand(
          "registration-request",
          new M.orchestration.ParticipationRequest(
            party.getValue,
            role.role,
            masterParty.getValue,
          ).create.commands.asScala.toSeq,
          reference,
          synchronizerId = Some(baseSynchronizerId),
        ),
        reference,
      )
      for {
        _ <- pr
        _ <-
          if (registerGenerator) {
            val reference = s"creating trade generator for $party"

            mapCommand(
              submitCommand(
                "trade-generator",
                new M.generator.Generator(
                  party.getValue,
                  List().asJava,
                ).create.commands.asScala.toSeq,
                reference,
              ),
              reference,
            )
          } else
            EitherT.rightT[Future, String](())
      } yield ()
    } else {
      EitherT.rightT(())
    }

  protected def ensureFlag(flag: M.orchestration.ParticipantFlag): Unit =
    testResult.one(()).foreach { res =>
      if (res.data.flag != flag) {
        updateFlag(res, flag)
      }
    }

  protected def updateFlag(
      participant: M.orchestration.TestParticipant.Contract,
      newFlag: M.orchestration.ParticipantFlag,
  ): Unit = {
    logger.info(s"Changing flag from ${participant.data.flag} to $newFlag")
    val cmd = participant.id
      .exerciseToggleFlag(newFlag)
      .commands
      .asScala
      .toSeq
    val submissionF =
      submitCommand(
        "update-flag",
        cmd,
        s"signalling to master change of my state from ${participant.data.flag} to $newFlag",
      )
    setPending(testResult, participant.id, submissionF)
  }

}

trait StatsUpdater {

  this: ParticipantDriver =>

  protected def sendStatsUpdates(
      res: M.orchestration.TestParticipant.Contract,
      master: M.orchestration.TestRun.Contract,
      proposalStats: TraderStats,
      acceptanceStats: TraderStats,
  ): Unit = {

    val prop = buildTestProbe(
      proposalStats,
      M.orchestration.ProbeType.PROPOSAL,
      res.data.proposed,
      master.data,
    )
    val accUp = buildTestProbe(
      acceptanceStats,
      M.orchestration.ProbeType.ACCEPTANCE,
      res.data.accepted,
      master.data,
    )

    if (prop.nonEmpty || accUp.nonEmpty) {
      def updateStr(typ: String, current: Long, probes: Seq[TestProbeData]): Seq[String] =
        probes.lastOption.map(x => s"$typ: $current -> ${x.count}").toList

      val textStr =
        (updateStr("proposal", res.data.proposed, prop) ++ updateStr(
          "accepts",
          res.data.accepted,
          accUp,
        ))
          .mkString(", ")

      val cmd = res.id.exerciseUpdateStats((prop ++ accUp).toList.asJava).commands.asScala.toSeq
      val submissionF = submitCommand("update-stats", cmd, s"Updating test results " + textStr)
      setPending(testResult, res.id, submissionF)
    }
  }

  private def buildTestProbe(
      stats: TraderStats,
      typ: M.orchestration.ProbeType,
      current: Long,
      master: M.orchestration.TestRun,
  ): Seq[M.orchestration.TestProbeData] = {
    val maxObs = stats.observed
    val obsIdx = Math.min(current + master.reportFrequency, master.totalCycles).toInt

    if (current < obsIdx && obsIdx <= maxObs) {
      val ts =
        stats
          .findTimestampAndCleanup(obsIdx)
      Seq(
        new M.orchestration.TestProbeData(
          typ,
          ts,
          obsIdx.toLong,
        )
      ) ++ buildTestProbe(stats, typ, obsIdx.toLong, master)
    } else
      Seq.empty
  }

  protected def updateTraderStats(
      mode: M.orchestration.Mode,
      proposalStats: TraderStats,
      proposalsOpen: Int,
      acceptanceStats: TraderStats,
      acceptsOpen: Int,
      issuerStats: Seq[(Party, Int, Int)],
      counterPartyStats: Seq[(Party, Int, Int)],
  ): Unit = {

    val lastStatsUpdate = currentStatus.get() match {
      case Some(ts: TraderStatus) => ts.timestamp
      case _ => Instant.MIN
    }
    if (lastStatsUpdate < Instant.now.minusMillis(500)) {
      currentStatus.set(
        Some(
          TraderStatus(
            name = name,
            timestamp = Instant.now,
            mode = mode.toString,
            currentRate = rate.currentRate,
            maxRate = rate.maxRate,
            latencyMs = rate.latencyMs,
            pending = rate.pending,
            backpressured = rate.backpressured,
            failed = rate.failed,
            proposals =
              StepStatus(proposalStats.submitted, proposalStats.observed, open = proposalsOpen),
            accepts =
              StepStatus(acceptanceStats.submitted, acceptanceStats.observed, open = acceptsOpen),
            counterParties = counterPartyStats,
            issuers = issuerStats,
          )
        )
      )
    }
  }

  protected def currentMaxTestResult(): (Int, Int) = {
    val (maxProposed, maxAccepted) = testResult.allAvailable.foldLeft((0, 0)) {
      case ((maxProposed, maxAccepted), inst) =>
        (
          Math.max(inst.data.proposed.toInt, maxProposed),
          Math.max(inst.data.accepted.toInt, maxAccepted),
        )
    }
    (maxProposed, maxAccepted)
  }

}
