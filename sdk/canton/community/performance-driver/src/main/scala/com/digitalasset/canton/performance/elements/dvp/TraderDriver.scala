// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements.dvp

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.javaapi.data.{Identifier, Party}
import com.daml.metrics.api.MetricHandle.{Gauge, LabeledMetricsFactory}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.performance.Connectivity
import com.digitalasset.canton.performance.PartyRole.DvpTrader
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.acs.ContractStore
import com.digitalasset.canton.performance.control.Balancer
import com.digitalasset.canton.performance.elements.dvp.DvpPrettyInstances.*
import com.digitalasset.canton.performance.elements.{
  DriverControl,
  ParticipantDriver,
  StatsUpdater,
  SubCommand,
}
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.model.java.dvp.trade.Propose
import com.digitalasset.canton.performance.model.java.orchestration.runtype.DvpRun
import com.digitalasset.canton.performance.model.java.orchestration.{Mode, TestRun}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ShowUtil.*
import com.typesafe.scalalogging.LazyLogging
import org.apache.pekko.actor.ActorSystem

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.jdk.CollectionConverters.*
import scala.util.{Random, Success}

class TraderStats(metric: Gauge[Long]) extends LazyLogging {

  private val submitted_ = new AtomicInteger(0)
  private val observed_ = new AtomicInteger(0)
  private val reports = mutable.ListBuffer[(Int, Instant)]()

  def init(count: Int): Unit = {
    submitted_.set(count)
    observed_.set(count)
    reports.append((count, Instant.now))
  }

  def submitted: Int = submitted_.get()
  def observed: Int = observed_.get()

  def incrementSubmitted(): Int = submitted_.incrementAndGet()
  def decrementSubmitted(): Int = submitted_.decrementAndGet()

  def incrementObserved(): Unit = {
    metric.updateValue(_ + 1)
    val current = observed_.incrementAndGet()
    // remember timestamps we need to report progress
    reports.append((current, Instant.now))
  }

  def findTimestampAndCleanup(observation: Int): Instant = {
    // find our report timestamp
    val idx = reports.indexWhere(_._1 == observation)
    // drop old reporting timestamps (we keep the current if we need to resubmit the stats)
    if (idx > 0) {
      reports.remove(0, idx)
    }
    // on a restart, we might have insufficient information which we approximate somehow
    reports.headOption.map(_._2).getOrElse(Instant.now)
  }

  override def toString: String = s"submitted=$submitted, observed=$observed"

}

/** @param connectivity
  *   Connectivity to the participant
  * @param partyLf
  *   Trader party (tradie)
  * @param masterLf
  *   Master party
  * @param role
  *   Role of the trader
  * @param prefix
  *   Prefix for the metrics
  * @param control
  *   Parameters for the performance runner
  * @param baseSynchronizerId
  *   Main synchronizer to be used (e.g., assets creation)
  * @param otherSynchronizers
  *   Other synchronizers that can be used to submit trades
  * @param otherSynchronizersRatio
  *   Ratio of trades that can be submitted to one of the `otherSynchronizers`
  */
class TraderDriver(
    connectivity: Connectivity,
    partyLf: LfPartyId,
    masterLf: LfPartyId,
    role: DvpTrader,
    prefix: MetricName,
    metricsFactory: LabeledMetricsFactory,
    loggerFactory: NamedLoggerFactory,
    control: DriverControl,
    baseSynchronizerId: SynchronizerId,
    otherSynchronizers: Seq[SynchronizerId],
    otherSynchronizersRatio: Double,
)(implicit
    val ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends ParticipantDriver(
      connectivity,
      partyLf,
      masterLf,
      role,
      registerGenerator = true,
      prefix,
      metricsFactory,
      loggerFactory,
      control,
      baseSynchronizerId,
    )
    with StatsUpdater {

  require(
    otherSynchronizersRatio >= 0.0 && otherSynchronizersRatio <= 1.0,
    s"otherSynchronizersRation should be in [0, 1]; found $otherSynchronizersRatio",
  )

  private val partyPoolSize = 1000;

  protected val assets = new ContractStore[
    M.dvp.asset.Asset.Contract,
    M.dvp.asset.Asset.ContractId,
    M.dvp.asset.Asset,
    Party,
  ](
    "assets i own",
    M.dvp.asset.Asset.COMPANION,
    index = x => new Party(x.data.issuer),
    filter = x => x.data.owner == party.getValue,
    loggerFactory,
  ) {
    override def update(before: M.dvp.asset.Asset.Contract): M.dvp.asset.Asset.Contract =
      new M.dvp.asset.Asset.Contract(
        before.id,
        new M.dvp.asset.Asset(
          before.data.registrar,
          before.data.issuer,
          before.data.id,
          // replace payload with payload string length to not run into OOM if we use large payloads
          before.data.payload.length.toString,
          before.data.owner,
          before.data.bystander,
        ),
        before.signatories,
        before.observers,
      )
  }
  private val assetRequests = new ContractStore[
    M.dvp.asset.AssetRequest.Contract,
    M.dvp.asset.AssetRequest.ContractId,
    M.dvp.asset.AssetRequest,
    Unit,
  ](
    "assets i requested",
    M.dvp.asset.AssetRequest.COMPANION,
    index = _ => (),
    filter = x => x.data.requester == party.getValue,
    loggerFactory,
  )

  protected val proposals = new ContractStore[
    M.dvp.trade.Propose.Contract,
    M.dvp.trade.Propose.ContractId,
    M.dvp.trade.Propose,
    Boolean,
  ](
    "proposals",
    M.dvp.trade.Propose.COMPANION,
    index = x => x.data.proposer == party.getValue,
    filter = _ => true,
    loggerFactory,
  ) {
    override protected def contractCreated(create: Propose.Contract, myProposal: Boolean): Unit =
      // increment observation of my proposal being created
      if (myProposal) {
        proposalStats.incrementObserved()
      }

    override protected def contractArchived(
        archive: Propose.Contract,
        myProposal: Boolean,
    ): Unit =
      // increment observation of my reaction to a proposal someone made to me
      if (!myProposal) {
        acceptanceStats.incrementObserved()
      } else {
        traderBalancer.completed(new Party(archive.data.counterparty))
        issuerBalancer.completed(new Party(archive.data.issuer))
      }
  }

  private val myProposalsOpen =
    metricsFactory.gauge[Long](
      MetricInfo(
        prefix :+ "proposals" :+ "open" :+ "mine",
        "My open proposals",
        MetricQualification.Debug,
      ),
      0,
    )(MetricsContext.Empty)

  private val theirProposalsOpen =
    metricsFactory.gauge[Long](
      MetricInfo(
        prefix :+ "proposals" :+ "open" :+ "theirs",
        "How many proposals the trader needs to confirm",
        MetricQualification.Debug,
      ),
      0,
    )(MetricsContext.Empty)

  private val proposalMetric =
    metricsFactory.gauge[Long](
      MetricInfo(
        prefix :+ "proposals",
        "How many open proposals the trader has outstanding",
        MetricQualification.Debug,
      ),
      0,
    )(
      MetricsContext.Empty
    ) // sum accepts and proposals together

  private val freeAssetsMetric = metricsFactory.gauge(
    MetricInfo(prefix :+ "freeAssets", "How many free assets", MetricQualification.Debug),
    0,
  )(MetricsContext.Empty)
  private val proposalStats = new TraderStats(proposalMetric)
  private val acceptanceStats = new TraderStats(proposalMetric)

  private val issuerBalancer = new Balancer()
  private val traderBalancer = new Balancer()

  private val bystanders = new ByStanderAllocation(
    party,
    role.name,
    ledgerClient,
    metricsFactory.gauge(
      MetricInfo(
        prefix :+ "parties" :+ "allocated",
        "How many parties have been allocated",
        MetricQualification.Debug,
      ),
      0L,
    )(
      MetricsContext.Empty
    ),
    maxPending = 10,
    loggerFactory,
  )

  override protected def masterCreated(master: TestRun.Contract): Unit = {
    issuerBalancer.updateMembers(master.data.issuers.asScala.toSeq.map(new Party(_)))
    traderBalancer.updateMembers(
      master.data.traders.asScala.toSeq.filter(_ != party.getValue).map(new Party(_))
    )
  }

  listeners.appendAll(Seq(assets, assetRequests, proposals))

  override protected def doInitExisting(): Boolean = {
    val (maxProposed, maxAccepted) = currentMaxTestResult()
    val numOpen = proposals.num(true)
    proposalStats.init(maxProposed + numOpen) // this is an approximate data.
    // we can't reconstruct the number of accepted items that we didn't "book". so we use an upper data
    // to ensure our traders don't think they should throttle
    acceptanceStats.init(Math.max(maxProposed, maxAccepted))
    logger.debug(
      s"Initialize max proposed with $maxProposed ($numOpen) and max accept with $maxAccepted"
    )
    proposalMetric.updateValue(
      Math.max(0, maxProposed + maxAccepted + numOpen - proposalMetric.getValue)
    )

    proposals.find(true).foreach { myProposal =>
      traderBalancer.adjust(new Party(myProposal.data.counterparty), 1)
      issuerBalancer.adjust(new Party(myProposal.data.issuer), 1)
    }
    true
  }

  override def flush(): Boolean =
    if (running.get()) {
      val reflush = masterContract
        .one(())
        .flatMap { master =>
          master.data.runType match {
            case dvp: DvpRun => Some((master, dvp))
            case va =>
              logger.error(s"This is a DvpRun trader while the test run uses $va ")
              None
          }
        }
        .map { case (master, params) =>
          updateTraderStats(
            master.data.mode,
            proposalStats,
            proposals.num(true),
            acceptanceStats,
            proposals.num(false),
            issuerBalancer.state(),
            traderBalancer.state(),
          )
          master.data.mode match {
            case Mode.PREPAREISSUER => false
            case Mode.PREPARETRADER =>
              initExisting()
              acquireAssets(master, params)
              signalReadyIfWeAre(master, params).discard
              false
            case Mode.THROUGHPUT =>
              initExisting()
              acquireAssets(master, params)
              if (signalReadyIfWeAre(master, params))
                checkConsistency(master, params)
              rate.updateRate(CantonTimestamp.now())
              val freeAssets =
                master.data.issuers.asScala.map(issuer => assets.num(new Party(issuer))).sum
              freeAssetsMetric.updateValue(freeAssets)
              val numOpenApprovals = proposals.num(false)

              val (maxNew, maxAccept) = TraderDriver.computeSubmissions(
                freeAssets = freeAssets,
                numOpenApprovals = numOpenApprovals,
                proposalsSubmitted = proposalStats.submitted,
                acceptanceSubmitted = acceptanceStats.submitted,
                totalCycles = master.data.totalCycles.toInt,
                submissionRateSettings = settings.submissionRateSettings,
                maxSubmissionPerIterationFactor = settings.factorOfMaxSubmissionsPerIteration,
                batchSize = settings.batchSize,
                pending = rate.pending,
                available = rate.available,
                maxRate = rate.maxRate,
                // throttle submissions if this trader is ahead by buffer * latency * batchSize of proposal submissions
                maxProposalsAheadBuffer = 6,
              )
              logger.debug(
                s"Proposals $proposalStats, Accepts $acceptanceStats, max-new: $maxNew, max-accept: $maxAccept, mine ${proposals
                    .num(true)}, theirs ${proposals
                    .num(false)}, inventory ${assets.totalNum}, rate $rate, pending: ${rate.pending}"
              )
              val cmdsP = createProposals(maxNew, master.data.issuers.size, params)
              val cmdsA = acceptProposals(maxAccept, params)
              // submit batches
              submitBatched(
                cmdsA ++ cmdsP,
                settings.batchSize,
                rate,
                settings.duplicateSubmissionDelay,
              )
              updateStats(master)
              signalFinishedIfWeAre(master)
              cmdsP.nonEmpty || cmdsA.nonEmpty
            case Mode.DONE =>
              finished()
              false
          }
        }
      reflush.getOrElse(false)
    } else {
      false
    }

  private def acceptProposals(maxAccept: Int, param: DvpRun): Seq[SubCommand] =
    proposals
      .find(false)
      .take(maxAccept)
      .flatMap { propose =>
        assets.allAvailable.headOption.toList.flatMap { asset =>
          val synchronizerId = {
            val useOtherSynchronizer =
              Math.random() <= otherSynchronizersRatio && otherSynchronizers.nonEmpty
            if (useOtherSynchronizer) {
              val idx = Math.floor(Math.random() * otherSynchronizers.size).toInt
              otherSynchronizers(idx)
            } else baseSynchronizerId
          }

          val cmd = propose.id
            .exerciseAccept(
              asset.id,
              bystanders.next(param.withPartyGrowth, partyPoolSize).getValue,
              Random.nextString(32),
            )
            .commands
            .asScala
            .headOption
            .getOrElse(throw new RuntimeException("no command was found"))

          val counter = acceptanceStats.incrementSubmitted()
          Seq(
            SubCommand(
              s"accept-$counter",
              show"accepting $counter from ${propose.data.proposer} with asset from ${asset.data.issuer}",
              Command.fromJavaProto(cmd.toProtoCommand),
              synchronizerId,
              submissionF => {
                setPending(proposals, propose.id, submissionF)
                setPending(assets, asset.id, submissionF)
              },
              () => {
                val _ = acceptanceStats.decrementSubmitted()
              },
            )
          )
        }
      }

  private val consistencyWarningIssued = new AtomicBoolean(false)
  private val shouldBeConsistent = new AtomicBoolean(false)
  private def checkConsistency(master: M.orchestration.TestRun.Contract, params: DvpRun): Unit =
    if (assetRequests.entirelyEmpty && proposalStats.observed > 0) {
      // assets can have two forms, just Assets or AssetTransfers. for every proposal
      // that we've created, there is an associated asset transfer, so counting
      // my proposals (as i already store that data)
      val numInTransfer = proposals.num(item = true)
      val assetsInconsistent =
        assets.totalNum + numInTransfer != params.numAssetsPerIssuer * master.data.issuers.size
      if (shouldBeConsistent.get()) {
        if (assetsInconsistent) {
          if (!consistencyWarningIssued.getAndSet(true)) {

            logger.error(
              s"Trader state appears to be inconsistent: assets=${assets.totalNum}, in-transfer=$numInTransfer, should be total=${params.numAssetsPerIssuer * master.data.issuers.size}" +
                s"Transfered-in proposals ${proposals.find(item = true)}, all proposals ${proposals.allAvailable}, all assets ${assets.allAvailable}"
            )
          }
        }
        // check for unaccounted commands (i.e. stuff that never completed)
        val threshold = Instant.now.minusSeconds(settings.commandExpiryCheckSeconds.toLong)
        val missing = pendingCommands.filter(_._2.isBefore(threshold)).keys
        if (missing.nonEmpty) {
          if (!consistencyWarningIssued.getAndSet(true)) {
            logger.error(
              s"Some commands should have finished long time ago but never did: $missing"
            )
          }
        }
        if (missing.isEmpty && !assetsInconsistent && consistencyWarningIssued.getAndSet(false)) {
          logger.error("Somehow recovered from inconsistency error?")
        }
      } else {
        shouldBeConsistent.set(!assetsInconsistent)
        if (!assetsInconsistent) {
          logger.info("Successfully added new issuers, number of assets is consistent again.")
        }
      }
    }

  private def createProposals(maxCreate: Int, numIssuers: Int, params: DvpRun): Seq[SubCommand] =
    generator.one(()).toList.flatMap { gen =>
      def go(count: Int, exhausted: Seq[Party]): Seq[SubCommand] =
        if (count == maxCreate || (exhausted.nonEmpty && exhausted.toSet.sizeIs == numIssuers)) {
          exhausted.foreach(issuerBalancer.completed) // unmark exhausted issuers
          Seq()
        } else {
          val issuer = issuerBalancer.next()
          assets.one(issuer) match {
            case Some(asset)
                if asset.data.payload != params.payloadSize.toString => // compare payload by strings (as we don't want to store all the data)
              issuerBalancer.completed(issuer)
              val cmd = asset.id
                .exerciseUpdatePayload(
                  Random.alphanumeric.take(params.payloadSize.toInt).mkString
                )
                .commands
                .asScala
                .headOption
                .getOrElse(throw new RuntimeException("no command was found"))
              val cidTxt = asset.id.toString.take(16)
              Seq(
                SubCommand(
                  s"adjust-payload-" + cidTxt,
                  show"adjusting payload of $cidTxt from ${asset.data.payload.length} to ${params.payloadSize}",
                  Command.fromJavaProto(cmd.toProtoCommand),
                  baseSynchronizerId,
                  submissionF => {
                    setPending(assets, asset.id, submissionF)
                  },
                  () => {},
                )
              ) ++ go(count + 1, exhausted)
            case Some(asset) =>
              val counter = proposalStats.incrementSubmitted()
              val counterParty = traderBalancer.next()

              val cmd =
                gen.id
                  .exerciseGenerateDvp(
                    asset.id,
                    counterParty.getValue,
                    params.withAcsGrowth,
                    bystanders.next(params.withPartyGrowth, partyPoolSize).getValue,
                  )
                  .commands
                  .asScala
                  .headOption
                  .getOrElse(throw new RuntimeException("no command was found"))
              Seq(
                SubCommand(
                  s"prop-$counter",
                  show"proposing $counter to ${counterParty.toString} with acs=${params.withAcsGrowth}",
                  Command.fromJavaProto(cmd.toProtoCommand),
                  baseSynchronizerId,
                  submissionF => {
                    setPending(assets, asset.id, submissionF)
                  },
                  () => {
                    // on command failure, update accounting
                    val res = proposalStats.decrementSubmitted()
                    logger.debug(s"Decrementing submitted to=$res after batch failure")
                    traderBalancer.completed(counterParty)
                    issuerBalancer.completed(issuer)
                  },
                )
              ) ++ go(count + 1, exhausted)
            case None =>
              go(count, exhausted :+ issuer)
          }
        }
      go(0, Seq())
    }

  private def updateStats(master: M.orchestration.TestRun.Contract): Unit = {
    // update statistics if necessary
    testResult.one(()).foreach { res =>
      sendStatsUpdates(res, master, proposalStats, acceptanceStats)
    }
    myProposalsOpen.updateValue(proposals.num(true).toLong)
    theirProposalsOpen.updateValue(proposals.num(false).toLong)
  }

  private def acquireAssets(master: M.orchestration.TestRun.Contract, params: DvpRun): Unit =
    generator.one(()).foreach { res =>
      // if there is a new issuer that we haven't seen before
      val issuers = master.data.issuers.asScala.toSeq.filterNot(res.data.processedIssuers.contains)
      if (issuers.nonEmpty) {
        val ids = issuers.map(_ => UUID.randomUUID().toString)
        val payload = Random.alphanumeric.take(params.payloadSize.toInt).mkString
        // the exercise will add the issuer to the generator, ensuring we don't request twice
        val cmd =
          res.id
            .exerciseRequestAssets(
              params.numAssetsPerIssuer,
              issuers.asJava,
              ids.asJava,
              payload,
            )
            .commands
            .asScala
            .toSeq
        val submissionF =
          submitCommand(
            "request-assets",
            cmd,
            s"acquiring ${params.numAssetsPerIssuer} assets from $issuers",
          )
        setPending(generator, res.id, submissionF)
        shouldBeConsistent.set(false)
        // unmark if command fails (we will retry)
        submissionF.onComplete {
          case Success(true) =>
          case x =>
            logger.warn(s"Requesting assets failed with $x, retrying")
        }
      }
    }

  private def signalFinishedIfWeAre(master: M.orchestration.TestRun.Contract): Unit =
    testResult.one(()).foreach { res =>
      // we are done if we submitted enough proposals and all proposals we are involved in vanished
      logger.debug(
        s"Current flag=${res.data.flag}, observed=${proposalStats.observed}, proposals-empty=${proposals.entirelyEmpty}"
      )
      if (
        res.data.flag == M.orchestration.ParticipantFlag.READY && proposalStats.observed >= master.data.totalCycles && proposals.entirelyEmpty
      ) {
        updateFlag(res, M.orchestration.ParticipantFlag.FINISHED)
      }
    }

  private def signalReadyIfWeAre(
      master: M.orchestration.TestRun.Contract,
      params: DvpRun,
  ): Boolean =
    testResult
      .one(())
      .exists { res =>
        if (res.data.flag == M.orchestration.ParticipantFlag.READY)
          true
        else if (res.data.flag == M.orchestration.ParticipantFlag.INITIALISING) {
          val expected = params.numAssetsPerIssuer * master.data.issuers.size
          val numInTransfer = proposals.num(item = true)
          val actual = assets.allAvailable.size + numInTransfer
          if (actual == expected) {
            // signal that we are ready if we have enough assets
            updateFlag(res, M.orchestration.ParticipantFlag.READY)
          } else {
            logger.info(
              s"Trader does not yet have the required number of assets (expected: $expected, actual: $actual)."
            )
          }
          false
        } else false
      }

  override protected def subscribeToTemplates: Seq[Identifier] =
    Seq(
      M.dvp.trade.Propose.TEMPLATE_ID,
      M.dvp.asset.Asset.TEMPLATE_ID,
      M.dvp.asset.AssetTransfer.TEMPLATE_ID,
      M.dvp.asset.AssetRequest.TEMPLATE_ID,
    )

}

object TraderDriver {

  /** computes the number of submissions we can do now
    * @return
    *   a tuple with num proposals, num accepts
    */
  def computeSubmissions(
      freeAssets: Int,
      numOpenApprovals: Int,
      proposalsSubmitted: Int,
      acceptanceSubmitted: Int,
      totalCycles: Int,
      submissionRateSettings: SubmissionRateSettings,
      maxSubmissionPerIterationFactor: Double,
      batchSize: Int,
      pending: Int,
      available: Int,
      maxRate: Double,
      maxProposalsAheadBuffer: Double,
  ): (Int, Int) = {

    val todo = Math.max(0, totalCycles - proposalsSubmitted)

    // how many commands can we submit according to the max rate
    val submit1 = available

    val submit2 = submissionRateSettings match {
      case targetLatency: SubmissionRateSettings.TargetLatency =>
        // number of pending (in-flight commands) should not exceed our current upper estimate of the max throughput
        // i.e. if our max throughput is 100, then having 500 pending transactions means that they will have roughly a latency of 5 seconds
        // as they are waiting in the queue to be processed. so 100 * target latency gives us the max number of pending commands we can have
        Math.min(
          submit1,
          Math.max(0, Math.max(1, maxRate * targetLatency.targetLatencyMs / 1000.0).toInt - pending),
        )

      case _: SubmissionRateSettings.FixedRate => submit1
    }

    // cap our submission at the max submission per iteration factor
    val submit3 = Math.min(submit2, Math.max(submit2 * maxSubmissionPerIterationFactor, 1).toInt)

    // available = currently available number of command submissions we can submit before we exceed the rate
    if (submit3 >= 1) {
      // determine how many commands we want to submit in this iteration. we only send a fraction of what we can per
      // iteration as we are iterating much faster than the system can process.
      val totalCmds = submit3 * batchSize
      val leaveAssetsForAccept = 5

      // first, we want to "keep up with others"
      val newProposals1 = Math.max(acceptanceSubmitted - proposalsSubmitted, 0)
      // second, but not more than we can submit in this round
      val newProposals2 = Math.min(newProposals1, totalCmds)
      // third, we can spend the excess bandwidth
      val newProposals3 = newProposals2 + Math.max(totalCmds - numOpenApprovals - newProposals2, 0)
      // fourth, we must not use up all free assets
      val newProposals4 = Math.min(newProposals3, Math.max(0, freeAssets - leaveAssetsForAccept))

      // fifth, if others are slow, then we have to stop submitting proposals until they catch up
      val newProposals5 = submissionRateSettings match {
        case targetLatency: SubmissionRateSettings.TargetLatency =>
          if (
            proposalsSubmitted - acceptanceSubmitted > maxProposalsAheadBuffer * batchSize * maxRate * (targetLatency.targetLatencyMs / 1000.0)
          ) {
            // prevent a deadlock if everything is quiet
            if (numOpenApprovals == 0 && pending == 0)
              Math.min(batchSize, newProposals4)
            else
              0
          } else newProposals4

        case _: SubmissionRateSettings.FixedRate =>
          // In this case, we hope the that others are not too slow (rate is low enough)
          newProposals4
      }

      // sixth, we shouldn't submit more than we have to
      val newProposals6 = Math.min(todo, newProposals5)
      require(totalCmds >= newProposals6)
      // seventh, we'll top up until we fill the batch
      val newProposals7 =
        Math.min(todo, newProposals6 + (batchSize - newProposals6 % batchSize) % batchSize)

      // now, to the approvals which is simple
      val newApprovals1 = Math.min(numOpenApprovals, totalCmds - newProposals6)
      // should not use up all free assets
      val newApprovals2 = Math.min(newApprovals1, Math.max(0, freeAssets - newProposals7))
      // should only submit according to batch size
      val newApprovals3 =
        if (todo > 0 && freeAssets > batchSize)
          newApprovals2 - newApprovals2 % batchSize
        else newApprovals2

      (newProposals7, newApprovals3)
    } else {
      (0, 0)
    }
  }
}
