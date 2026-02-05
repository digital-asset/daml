// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.event.{ArchivedEvent, CreatedEvent}
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.performance.PartyRole.MasterDynamicConfig
import com.digitalasset.canton.performance.acs.{ContractObserver, ContractStore}
import com.digitalasset.canton.performance.elements.DriverStatus.MasterStatus
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.model.java.orchestration.runtype.DvpRun
import com.digitalasset.canton.performance.model.java.orchestration.{ProbeType, Role, TestRun}
import com.digitalasset.canton.performance.{Connectivity, PartyRole}
import com.digitalasset.canton.util.ErrorUtil
import org.apache.pekko.actor.ActorSystem

import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.*

/** Trait to amend the current run config when it hits a certain condition */
abstract class AmendMasterConfig(val description: String) {
  def check(stats: MasterStatus, master: M.orchestration.TestRun): Option[MasterDynamicConfig]
}

object AmendMasterConfig {
  abstract class AmendMasterConfigDvp(description: String) extends AmendMasterConfig(description) {
    def checkDvp(
        stats: MasterStatus,
        master: M.orchestration.TestRun,
        dvp: M.orchestration.runtype.DvpRun,
    ): Option[MasterDynamicConfig]

    override def check(stats: MasterStatus, master: TestRun): Option[MasterDynamicConfig] =
      master.runType match {
        case x: DvpRun => checkDvp(stats, master, x)
        case _ => None
      }
  }

  class UpdatePayloadSize(target: Long) extends AmendMasterConfigDvp("update payload size") {
    override def checkDvp(
        stats: MasterStatus,
        master: TestRun,
        dvp: DvpRun,
    ): Option[MasterDynamicConfig] =
      Some(
        MasterDynamicConfig(
          totalCycles = master.totalCycles.toInt,
          reportFrequency = master.reportFrequency.toInt,
          runType = new DvpRun(
            dvp.numAssetsPerIssuer,
            dvp.withAcsGrowth,
            target,
            dvp.partyGrowth,
          ),
        )
      )
  }
}

class MasterDriver(
    connectivity: Connectivity,
    masterPartyLf: LfPartyId,
    config: PartyRole.Master,
    loggerFactory: NamedLoggerFactory,
    control: DriverControl,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends BaseDriver(
      connectivity,
      masterPartyLf,
      masterPartyLf,
      config.commandClientConfiguration,
      loggerFactory,
      control,
    ) {

  def name: String = config.name

  def addConfigAdjustment(amender: AmendMasterConfig): Unit =
    amendedConfig.updateAndGet(x => x :+ amender).discard

  private val requests = new ContractStore[
    M.orchestration.ParticipationRequest.Contract,
    M.orchestration.ParticipationRequest.ContractId,
    M.orchestration.ParticipationRequest,
    Party,
  ](
    "participant requests",
    M.orchestration.ParticipationRequest.COMPANION,
    index = x => new Party(x.data.party),
    filter = _ => true,
    loggerFactory,
  )

  private val amendedConfig = new AtomicReference[Seq[AmendMasterConfig]](config.amendments)

  private val statsUpdateNeeded = new AtomicBoolean(false)
  private val testProbes =
    new ContractStore[
      M.orchestration.TestProbe.Contract,
      M.orchestration.TestProbe.ContractId,
      M.orchestration.TestProbe,
      (Party, M.orchestration.ProbeType),
    ](
      "test probes",
      M.orchestration.TestProbe.COMPANION,
      index = x => (new Party(x.data.party), x.data.typ),
      filter = x => x.data.master == masterParty.getValue,
      loggerFactory,
    )

  private val testParticipants =
    new ContractStore[
      M.orchestration.TestParticipant.Contract,
      M.orchestration.TestParticipant.ContractId,
      M.orchestration.TestParticipant,
      Party,
    ](
      "test participant",
      M.orchestration.TestParticipant.COMPANION,
      index = x => new Party(x.data.party),
      filter = x => x.data.master == masterParty.getValue,
      loggerFactory,
    )

  private def templateMatches(expected: javaapi.data.Identifier)(actual: Identifier): Boolean =
    Identifier.fromJavaProto(expected.toProto) == actual

  listeners.appendAll(
    Seq(
      requests,
      testParticipants,
      testProbes,
      new ContractObserver() {
        override def processCreate(create: CreatedEvent): Boolean = {
          if (
            create.templateId.fold(false)(
              templateMatches(M.orchestration.TestProbe.TEMPLATE_ID_WITH_PACKAGE_ID)(_)
            ) ||
            create.templateId
              .fold(false)(templateMatches(M.orchestration.TestRun.TEMPLATE_ID_WITH_PACKAGE_ID)(_))
          ) {
            statsUpdateNeeded.set(true)
          }
          false
        }
        override def processArchive(archive: ArchivedEvent): Boolean = false
        override def reset(): Unit = {}
      },
    )
  )

  protected def subscribeToTemplates: Seq[javaapi.data.Identifier] = Seq()

  override def start(): Future[Either[String, Unit]] =
    (for {
      _ <- EitherT(super.start())
      _ <- masterContract.allAvailable.headOption match {
        case None =>
          logger.debug("Adding new test run contract")
          mapCommand(setupTestRun(), "setup test run")
        case Some(_) =>
          logger.debug("Resuming with existing test run contract")
          EitherT.rightT[Future, String](())
      }
    } yield ()).value

  private def setupTestRun(): Future[Boolean] =
    submitCommand(
      "setup-test-run",
      new M.orchestration.TestRun(
        M.orchestration.Mode.PREPAREISSUER,
        List().asJava,
        List().asJava,
        party.getValue,
        config.runConfig.totalCycles.toLong,
        config.runConfig.reportFrequency.toLong,
        config.runConfig.runType,
      ).create.commands.asScala.toSeq,
      "Test Run Orchestration",
    ).flatMap { success =>
      if (!success && !isClosing) {
        logger.warn("Test run orchestration initialisation failed. Retrying")
        Threading.sleep(2000)
        setupTestRun()
      } else {
        Future.successful(success)
      }
    }

  private def updateTestRunConfigIfNecessary(): Unit =
    (for {
      newConfigChecker <- amendedConfig.get().headOption
      master <- masterContract.one(())
      stats <- currentStatus.get().collect { case x: MasterStatus => x }
      updated <- newConfigChecker.check(stats, master.data)
    } yield {
      if (updated != MasterDynamicConfig.fromContract(master.data)) {
        logger.info(s"Adjusting test run config to $updated")
        val amendMaster = master.id
          .exerciseToggleRunType(
            updated.runType,
            updated.totalCycles.toLong,
            updated.reportFrequency.toLong,
          )
          .commands
          .asScala
          .toSeq
        val submissionF = submitCommand(
          "amend-master",
          amendMaster,
          s"Amending master run-type",
        )
        setPending(masterContract, master.id, submissionF)
      } else {
        logger.info("Dropping amender as we are on it")
        // dropping adjuster now that we are set and have already adjusted the master contract
        amendedConfig.updateAndGet(
          _.drop(1)
        )
      }
    }).discard

  override def flush(): Boolean = {
    requests.allAvailable.headOption // can only process one at the time
      .flatMap(request =>
        // process participation requests
        masterContract.one(()).map { master =>
          val acceptParticipation =
            request.id
              .exerciseAccept(master.id)
              .commands
              .asScala
              .toSeq
          val submissionF = submitCommand(
            "accept-participation",
            acceptParticipation,
            s"registering participant ${request.data.party}",
          )
          setPending(masterContract, master.id, submissionF)
          setPending(requests, request.id, submissionF)
        }
      )
      .discard
    updateTestRunConfigIfNecessary()
    if (statsUpdateNeeded.getAndSet(false)) {
      updateStatus()
    }
    // adapt state based on progress
    adaptTestRunState()
    false
  }

  private def cleanupTestProbes(cleanup: List[M.orchestration.TestProbe.Contract]): Unit =
    cleanup.foreach { probe =>
      val command = probe.id
        .exerciseAcknowledge()
        .commands
        .asScala
        .toSeq
      val submissionF = submitCommand(
        "acknowledge-test-probe",
        command,
        s"Acknowledging test probe from ${probe.data.party} of type ${probe.data.typ} at ${probe.data.count}",
      )
      setPending(testProbes, probe.id, submissionF)
    }

  private def calculateStatistics(): Option[(Double, Double, Long, Long)] =
    masterContract.one(()).map { master =>
      val stats: Seq[(String, ProbeType, Long, Double)] =
        master.data.traders.asScala.toSeq.flatMap { trader =>
          Seq(M.orchestration.ProbeType.ACCEPTANCE, M.orchestration.ProbeType.PROPOSAL).map { typ =>
            // reverse sort by epoch milli (Ordering.by(_.data.timestamp).reverse fails due to diverging implicit expansion
            testProbes.find((new Party(trader), typ)).sortBy(-_.data.count) match {
              case last :: prev :: rest =>
                import scala.Ordered.orderingToOrdered
                cleanupTestProbes(rest)
                // naive evaluation of tps by looking at last intervals
                ErrorUtil.requireState(
                  (prev.data.timestamp: Instant) <= (last.data.timestamp: Instant),
                  s"${prev.data} is after ${last.data}",
                )

                val lastCount = last.data.count
                val throughput =
                  if (
                    lastCount < master.data.totalCycles || !config.runConfig.zeroTpsReportOnFinish
                  ) {
                    val duration =
                      Math
                        .max(1, last.data.timestamp.toEpochMilli - prev.data.timestamp.toEpochMilli)
                    (lastCount - prev.data.count) * 1000.0 / duration
                  } else {
                    // Report zero throughput for the last interval to make sure that the total throughput goes down to zero
                    // when the test has completed.
                    // This effectively discards the throughput resulting from the last test probe,
                    // but it is a very simple solution.
                    0.0
                  }
                (trader, typ, lastCount, throughput)
              case _ => (trader, typ, 0L, 0.0)
            }
          }
        }
      val (accTot, accPs) = stats
        .filter(_._2 == M.orchestration.ProbeType.ACCEPTANCE)
        .foldLeft((0L, 0.0))((acc, elem) => (acc._1 + elem._3, acc._2 + elem._4))
      val (proTot, proPs) = stats
        .filter(_._2 == M.orchestration.ProbeType.PROPOSAL)
        .foldLeft((0L, 0.0))((acc, elem) => (acc._1 + elem._3, acc._2 + elem._4))
      (accPs, proPs, accTot, proTot)
    }

  private def updateStatus(): Unit =
    calculateStatistics().foreach { case (accPs, proPs, accTot, proTot) =>
      masterContract.one(()).map { master =>
        val newStatus = MasterStatus(
          timestamp = Instant.now,
          mode = master.data.mode.toString,
          proposalsPerSecond = proPs,
          approvalsPerSecond = accPs,
          totalProposals = proTot,
          totalApprovals = accTot,
        )

        logger.info(newStatus.toString)
        currentStatus.set(Some(newStatus))
      }
    }

  private def adaptTestRunState(): Unit =
    // only run if master contract is not pending
    masterContract.one(()).foreach { _ =>
      // if everyone is done, disable
      if (runCompleted()) {
        adjustState(M.orchestration.Mode.DONE)
      }
      // if trader quorum is reached and enough are ready, start
      else if (tradersAreReady()) {
        adjustState(M.orchestration.Mode.THROUGHPUT)
      }
      // if issuer quorum is reached, prepare
      else if (issuersAreReady()) {
        adjustState(M.orchestration.Mode.PREPARETRADER)
      }
    }

  private def runCompleted(): Boolean = {
    val (notDone, doneIssuer, doneTrader) = testParticipants.allAvailable.foldLeft((0, 0, 0)) {
      case ((notDone, doneIssuer, doneTrader), tp) =>
        if (tp.data.flag == M.orchestration.ParticipantFlag.FINISHED)
          if (tp.data.role == Role.TRADER)
            (notDone, doneIssuer, doneTrader + 1)
          else
            (notDone, doneIssuer + 1, doneTrader)
        else
          (notDone + 1, doneIssuer, doneTrader)
    }
    notDone == 0 && doneIssuer >= config.quorumIssuers && doneTrader >= config.quorumParticipants
  }

  private def tradersAreReady(): Boolean = {
    val count = testParticipants.allAvailable
      .count(x =>
        x.data.role == Role.TRADER && x.data.flag != M.orchestration.ParticipantFlag.INITIALISING
      )
    count >= config.quorumParticipants
  }

  private def issuersAreReady(): Boolean = {
    val count = testParticipants.allAvailable
      .count(x =>
        x.data.role == Role.ISSUER && x.data.flag != M.orchestration.ParticipantFlag.INITIALISING
      )
    count >= config.quorumIssuers
  }

  private def adjustState(mode: M.orchestration.Mode): Unit =
    masterContract.one(()).foreach { master =>
      if (master.data.mode != mode && master.data.mode != M.orchestration.Mode.DONE) {
        logger.info(s"Change mode from ${master.data.mode} to $mode")
        val command = master.id
          .exerciseToggleMode(mode)
          .commands
          .asScala
          .toSeq
        val submissionF =
          submitCommand("toggle-mode", command, s"Changing mode ${master.data.mode} => $mode")
        setPending(masterContract, master.id, submissionF)
      }
      if (master.data.mode == M.orchestration.Mode.DONE) {
        finished()
      }
    }

}
