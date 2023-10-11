// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.implicits.toFoldableOps
import cats.syntax.parallel.*
import com.daml.ledger.api.refinements.ApiTypes.WorkflowId
import com.daml.ledger.api.v1.commands.Command as ScalaCommand
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.client.binding.{Contract, Primitive as P}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.error.ErrorCodeUtils
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.ContractNotFound
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  Lifecycle,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.admin.workflows.{PingPong as M, PingPongVacuum as V}
import com.digitalasset.canton.participant.ledger.api.client.{
  CommandResult,
  DecodeUtil,
  LedgerAcs,
  LedgerConnection,
}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.sync.TransactionRoutingError.TopologyErrors.UnknownInformees
import com.digitalasset.canton.protocol.messages.LocalReject.ConsistencyRejections.{
  InactiveContracts,
  LockedContracts,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.util.{
  BatchAggregator,
  FutureUtil,
  LoggerUtil,
  SimpleExecutionQueue,
  retry,
}
import com.google.common.annotations.VisibleForTesting
import com.google.rpc.code.Code.DEADLINE_EXCEEDED
import org.slf4j.event.Level
import scalaz.Tag

import java.time.Duration
import java.util.UUID
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap
import scala.concurrent.*
import scala.concurrent.duration.DurationLong
import scala.math.min
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** Implements the core of the ledger ping service for a participant.
  * The service measures the time needed for a nanobot on the responder to act on a contract created by the initiator.
  *
  * The main functionality:
  * 1. once instantiated, it automatically starts a Scala Nanobot that responds to pings for this participant
  * 2. it provides a ping method that sends a ping to the given (target) party
  *
  * Parameters:
  * @param adminPartyId PartyId            the party on whose behalf to send/respond to pings
  * @param maxLevelSupported Long          the maximum level we will participate in "Explode / Collapse" Pings
  * @param loggerFactory NamedLogger       logger
  * @param clock Clock                     clock for regular garbage collection of duplicates and merges
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class PingService(
    connection: LedgerAcs,
    adminPartyId: PartyId,
    maxLevelSupported: Long,
    override protected val timeouts: ProcessingTimeout,
    pingDeduplicationTime: NonNegativeFiniteDuration,
    isActive: => Boolean,
    syncService: Option[CantonSyncService],
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
    protected val clock: Clock,
)(implicit ec: ExecutionContext, timeoutScheduler: ScheduledExecutorService)
    extends AdminWorkflowService
    with FlagCloseable
    with HasCloseContext
    with NamedLogging {
  private val adminParty = adminPartyId.toPrim

  // Used to synchronize the ping requests and responses.
  // Once the promise is fulfilled, the ping for the given id is complete.
  // The result String contains of the future yields the party that responded. The string indicates if a
  // response was already received from a certain sender. We use this to check within a grace period for "duplicate spends".
  // So when a response was received, we don't end the future, but we wait for another `grace` period if there is
  // a second, invalid response.
  private val responses: TrieMap[String, (Option[String], Option[Promise[String]])] = new TrieMap()

  private case class DuplicateIdx(pingId: String, keyId: String)
  private val duplicate = TrieMap.empty[DuplicateIdx, Unit => String]

  private case class MergeIdx(pingId: String, path: String)
  private case class MergeItem(merge: Contract[M.Merge], first: Option[Contract[M.Collapse]])
  private val merges: TrieMap[MergeIdx, MergeItem] = new TrieMap()

  private val DefaultCommandTimeoutMillis: Long = 5 * 60 * 1000

  private val vacuumWorkflowId = WorkflowId("vacuuming")

  // Execution queue for the vacuuming tasks
  private val vacuumQueue = new SimpleExecutionQueue(
    "ping-service-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  private val packageReadyFU = {
    import TraceContext.Implicits.Empty.*
    implicit val success = retry.Success((x: Boolean) => x)
    retry
      .Backoff(
        logger,
        this,
        retry.Forever,
        100.millis,
        2.seconds,
        "wait-for-admin-workflows-to-appear-on-ledger-api",
      )
      .unlessShutdown(
        performUnlessClosingF("wait-for-admin-workflows-to-appear-on-ledger-api")(
          connection
            .getPackageStatus(Tag.unwrap(M.Ping.id).packageId)
            .map(_.packageStatus.isRegistered)
        ),
        AllExnRetryable,
      )
      .map { _ => () }
  }

  // Execute vacuuming task when (re)connecting to a new domain
  syncService.foreach(
    _.subscribeToConnections { domainAlias =>
      withNewTraceContext { implicit traceContext =>
        logger.debug(s"Received connection notification from $domainAlias")
        FutureUtil.doNotAwait(
          vacuumQueue
            .execute(vacuumStaleContracts, "Ping vacuuming")
            .onShutdown(logger.debug("Aborted ping vacuuming due to shutdown")),
          "Failed to execute Ping vacuuming task",
        )
      }
    }
  )

  // TransactionFilter to ensure the vacuuming does not operate on unwanted contracts
  private val vacuumFilter = {
    val templateIds = Seq(
      M.PingProposal.id,
      M.Ping.id,
      M.Pong.id,
      M.Explode.id,
      M.Merge.id,
      M.Collapse.id,
    ).map(LedgerConnection.mapTemplateIds(_))

    LedgerConnection.transactionFilterByParty(Map(adminPartyId -> templateIds))
  }

  case class VacuumCommand(id: String, action: String, command: ScalaCommand)
      extends PrettyPrinting {
    override def pretty: Pretty[VacuumCommand] =
      prettyOfClass(
        param("id", _.id.doubleQuoted),
        param("action", _.action.doubleQuoted),
        param("command", _.command.toString.unquoted),
      )
  }

  private val vacuumBatchAggregator = {
    val config = BatchAggregatorConfig(
      maximumInFlight = PositiveNumeric.tryCreate(1),
      maximumBatchSize = PositiveNumeric.tryCreate(50),
    )

    val processor: BatchAggregator.Processor[VacuumCommand, Unit] =
      new BatchAggregator.Processor[VacuumCommand, Unit] {
        override val kind: String = "Ping vacuuming command"

        override def logger: TracedLogger = PingService.this.logger

        override def executeBatch(items: NonEmpty[Seq[Traced[VacuumCommand]]])(implicit
            traceContext: TraceContext,
            callerCloseContext: CloseContext,
        ): Future[Seq[Unit]] = {
          Future.traverse(items.forgetNE)(item => {
            submitIgnoringErrors(
              item.value.id,
              item.value.action,
              item.value.command,
              Some(vacuumWorkflowId),
              NoCommandDeduplicationNeeded,
              unknownInformeesLogLevel = Level.INFO,
            )
          })
        }

        override def prettyItem: Pretty[VacuumCommand] = implicitly
      }

    BatchAggregator(
      processor,
      config,
      None,
    )
  }

  /** Vacuum stale Ping/Pong contracts
    *
    * Use a "best effort" approach, mostly limited to archiving contracts if possible, or somehow advancing
    * the workflows if we can.
    * Try to limit Bong explosions by atomically responding to our own stale pings and eliminating the
    * resulting contracts (see `PingPongVacuum` Daml module).
    */
  private def vacuumStaleContracts(implicit traceContext: TraceContext): Future[Unit] =
    packageReadyFU
      .flatMap { _ =>
        // TODO(i10722): To be improved when a better multi-domain API is available
        performUnlessClosingF("Ping vacuuming")(for {
          (activeContracts, offset) <- connection.activeContracts(vacuumFilter)
          _ = logger.debug(
            s"Attempting to vacuum ${activeContracts.size} active PingService contract(s) ; offset = $offset"
          )

          _ <- Seq(
            vacuumPingProposals(activeContracts),
            vacuumPings(activeContracts),

            // Process the Pong contracts normally
            processPongsF(
              activeContracts.flatMap(DecodeUtil.decodeCreated(M.Pong)(_)),
              vacuumWorkflowId,
              unknownInformeesLogLevel = Level.INFO,
            ),
            vacuumExplodes(activeContracts),
            vacuumMerges(activeContracts),
            vacuumCollapses(activeContracts),
          ).sequence_
        } yield ())
      }
      .onShutdown(())

  private def vacuumPingProposals(
      activeContracts: Seq[CreatedEvent]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val pingProposals = activeContracts.flatMap(DecodeUtil.decodeCreated(M.PingProposal)(_))

    val (toArchive, toProcess) = pingProposals.partition(contract =>
      (contract.value.initiator == adminParty) && contract.value.validators.forall(_ == adminParty)
    )

    // Archive the ones we can
    val futArchive = toArchive.parTraverse_ { contract =>
      logger.debug(s"archiving PingProposal $contract")

      vacuumBatchAggregator.run(
        VacuumCommand(
          contract.value.id,
          s"$adminParty archiving PingProposal",
          contract.contractId.exerciseArchive().command,
        )
      )
    }

    // Try to process the remaining ones
    val futProcess =
      processProposalsF(toProcess, vacuumWorkflowId, unknownInformeesLogLevel = Level.INFO)

    Seq(futArchive, futProcess).sequence_
  }

  private def vacuumPings(
      activeContracts: Seq[CreatedEvent]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val pings = activeContracts.flatMap(DecodeUtil.decodeCreated(M.Ping)(_))

    val (toArchive, toCleanup) =
      pings
        .filter(_.value.initiator == adminParty)
        .partition(_.value.validators.forall(_ == adminParty))

    // Archive the ones we can
    val futArchive = toArchive.parTraverse_ { contract =>
      logger.debug(s"archiving Ping $contract")

      vacuumBatchAggregator.run(
        VacuumCommand(
          contract.value.id,
          s"$adminParty archiving Ping",
          contract.contractId.exerciseArchive().command,
        )
      )
    }

    // Try to clean the remaining ones
    val futCleanup = toCleanup.parTraverse_ { contract =>
      logger.debug(s"cleaning Ping $contract")

      vacuumBatchAggregator.run(
        VacuumCommand(
          "ping-cleanup",
          s"$adminParty cleaning Ping",
          V.PingCleanup(adminParty, contract.contractId).createAnd.exerciseProcess().command,
        )
      )
    }

    Seq(futArchive, futCleanup).sequence_
  }

  private def vacuumExplodes(
      activeContracts: Seq[CreatedEvent]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val explodes = activeContracts.flatMap(DecodeUtil.decodeCreated(M.Explode)(_))

    val toExpire = explodes.filter(_.value.initiator == adminParty)

    // Archive the ones we can
    toExpire.parTraverse_ { contract =>
      logger.debug(s"expiring Explode $contract")

      vacuumBatchAggregator.run(
        VacuumCommand(
          contract.value.id,
          s"$adminParty expiring Explode",
          contract.contractId.exerciseExpireExplode().command,
        )
      )
    }
  }

  private def vacuumMerges(
      activeContracts: Seq[CreatedEvent]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val merges = activeContracts.flatMap(DecodeUtil.decodeCreated(M.Merge)(_))

    val toExpire = merges.filter(_.value.initiator == adminParty)

    // Expire the ones we can
    toExpire.parTraverse_ { contract =>
      logger.debug(s"expiring Merge $contract")

      vacuumBatchAggregator.run(
        VacuumCommand(
          contract.value.id,
          s"$adminParty expiring Merge",
          contract.contractId.exerciseExpireMerge().command,
        )
      )
    }
  }

  private def vacuumCollapses(
      activeContracts: Seq[CreatedEvent]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val collapses = activeContracts.flatMap(DecodeUtil.decodeCreated(M.Collapse)(_))

    val toExpire = collapses.filter(_.value.initiator == adminParty)

    // Expire the ones we can
    toExpire.parTraverse_ { contract =>
      logger.debug(s"expiring Collapse $contract")

      vacuumBatchAggregator.run(
        VacuumCommand(
          contract.value.id,
          s"$adminParty expiring Collapse",
          contract.contractId.exerciseExpireCollapse().command,
        )
      )
    }
  }

  /** The command deduplication time for commands that don't need deduplication because
    * any repeated submission would fail anyway, say because the command exercises a consuming choice on
    * a specific contract ID (not contract key).
    */
  private def NoCommandDeduplicationNeeded: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfMillis(1)

  /** Send a ping to the target party, return round-trip time or a timeout
    * @param targetParties String     the parties to send ping to
    * @param validators additional validators (signatories) of the contracts
    * @param timeoutMillis Long     how long to wait for pong (in milliseconds)
    */
  def ping(
      targetParties: Set[String],
      validators: Set[String],
      timeoutMillis: Long,
      duplicateGracePeriod: Long = 1000,
      maxLevel: Long = 0,
      workflowId: Option[WorkflowId] = None,
      id: String = UUID.randomUUID().toString,
  )(implicit traceContext: TraceContext): Future[PingService.Result] =
    packageReadyFU
      .flatMap { _ =>
        FutureUnlessShutdown.outcomeF {
          logger.debug(s"Sending ping $id from $adminParty to $targetParties")
          val promise = Promise[String]()
          val resultPromise: (Option[String], Option[Promise[String]]) =
            (None, Some(promise))
          responses += (id -> resultPromise)
          if (maxLevel > maxLevelSupported) {
            logger.warn(s"Capping max level $maxLevel to $maxLevelSupported")
          }
          val start = System.nanoTime()
          val result = for {
            _ <- submitPing(
              id,
              targetParties,
              validators,
              min(maxLevel, maxLevelSupported),
              workflowId,
              timeoutMillis,
            )
            response <- timeout(promise.future, timeoutMillis)
            end = System.nanoTime()
            rtt = Duration.ofNanos(end - start)
            _ = if (targetParties.size > 1)
              logger.debug(s"Received ping response from $response within $rtt")
            successMsg = PingService.Success(rtt, response)
            success <- responses.get(id) match {
              case None =>
                // should not happen as this here is the only place where we remove them
                Future.failed(new RuntimeException("Ping disappeared while waiting"))
              case Some((Some(`response`), Some(gracePromise))) =>
                // wait for grace period
                if (targetParties.size > 1) {
                  // wait for grace period to expire. if it throws, we are good, because we shouldn't get another responsefailed with reason
                  timeout(gracePromise.future, duplicateGracePeriod) transform {
                    // future needs to timeout. otherwise we received a duplicate spent
                    case Failure(_: TimeoutException) => Success(successMsg)
                    case Success(x) =>
                      logger.debug(s"gracePromise for Ping $id resulted in $x. Expected a Timeout.")
                      Success(PingService.Failure)
                    case Failure(ex) =>
                      logger.debug(s"gracePromise for Ping $id threw unexpected exception.", ex)
                      Failure(ex)
                  }
                } else Future.successful(successMsg)
              case Some(x) =>
                logger.debug(s"Ping $id response was $x. Expected (Some, Some).")
                Future.successful(PingService.Failure)
            }
            _ = logger.debug(s"Ping test $id resulted in $success, deregistering")
            _ = responses -= id
          } yield success

          result transform {
            case Failure(_: TimeoutException) =>
              responses -= id
              Success(PingService.Failure)
            case other => other
          }
        }
      }
      .onShutdown(PingService.Failure)

  override protected def onClosed(): Unit = Lifecycle.close(connection, vacuumQueue)(logger)

  private def submitPing(
      id: String,
      responders: Set[String],
      validators: Set[String],
      maxLevel: Long,
      workflowId: Option[WorkflowId],
      timeoutMillis: Long,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    if (validators.isEmpty) {
      logger.debug(s"Starting ping $id with responders $responders and level $maxLevel")
      val ping = M.Ping(id, adminParty, List.empty, responders.map(P.Party(_)).toList, maxLevel)
      submitIgnoringErrors(
        id,
        "ping",
        ping.create.command,
        workflowId,
        pingDeduplicationTime,
        timeoutMillis,
      )
    } else {
      logger.debug(
        s"Proposing ping $id with responders $responders, validators $validators and level $maxLevel"
      )
      val ping = M.PingProposal(
        id = id,
        initiator = adminParty,
        candidates = validators.map(P.Party(_)).toList,
        validators = List.empty,
        responders = responders.map(P.Party(_)).toList,
        maxLevel = maxLevel,
      )
      submitIgnoringErrors(
        id,
        "ping-proposal",
        ping.create.command,
        workflowId,
        pingDeduplicationTime,
        timeoutMillis,
      )
    }
  }

  @nowarn("msg=match may not be exhaustive")
  private def submitIgnoringErrors(
      id: String,
      action: String,
      cmd: ScalaCommand,
      workflowId: Option[WorkflowId],
      deduplicationDuration: NonNegativeFiniteDuration,
      timeoutMillis: Long = DefaultCommandTimeoutMillis,
      unknownInformeesLogLevel: Level = Level.WARN,
  )(implicit traceContext: TraceContext): Future[Unit] = {

    val desc = s"Ping/pong with id=$id-$action"
    // Include a random UUID in the command id, as id/action are not necessarily unique.
    // - The same participant may submit several pings with the same ID.
    // - A ping may get duplicated by CommandSubmitterWithRetry
    val commandId = s"$id-$action-${UUID.randomUUID()}"

    timeout(
      connection.submitCommand(
        Seq(cmd),
        Some(commandId),
        workflowId,
        deduplicationTime = Some(deduplicationDuration),
        timeout = Some(Duration.ofMillis(timeoutMillis)),
      ),
      timeoutMillis,
    ).transform { res =>
      res match {
        // Some failures are logged below at INFO level as we sometimes do a ping expecting that it will fail.
        case Failure(_: TimeoutException) =>
          logger.info(s"$desc: no completion received within timeout of ${timeoutMillis.millis}.")
        case Success(CommandResult.Failed(_, status)) if status.code == DEADLINE_EXCEEDED.value =>
          logger.info(s"$desc failed with reason $status")
        case Success(CommandResult.Success(_)) =>
          logger.debug(s"$desc succeeded.")
        case Success(CommandResult.Failed(_, errorStatus))
            if Seq(ContractNotFound, InactiveContracts, LockedContracts).exists(
              ErrorCodeUtils.isError(errorStatus.message, _)
            ) =>
          logger.info(s"$desc failed with reason $errorStatus.")
        case Success(CommandResult.Failed(_, errorStatus))
            if ErrorCodeUtils.isError(errorStatus.message, UnknownInformees) =>
          // UNKNOWN_INFORMEES can be triggered by the Ping vacuuming process in multi-domain settings.
          // In these situations, we should log it at a lower severity because it is expected.
          LoggerUtil.logAtLevel(
            unknownInformeesLogLevel,
            s"$desc failed with reason $errorStatus.",
          )
        case Success(CommandResult.AbortedDueToShutdown) =>
          logger.info(s"$desc failed due to service shutdown")
        case Success(reason) =>
          logger.warn(s"$desc failed with reason $reason.")
        case Failure(ex) if NonFatal(ex) =>
          logger.warn(s"$desc failed due to an internal error.", ex)
      }
      Success(())
    }
  }

  private def submitAsync(
      id: String,
      action: String,
      cmd: ScalaCommand,
      workflowId: Option[WorkflowId],
      deduplicationDuration: NonNegativeFiniteDuration,
      timeoutMillis: Long = DefaultCommandTimeoutMillis,
  )(implicit traceContext: TraceContext): Unit =
    FutureUtil.doNotAwait(
      submitIgnoringErrors(id, action, cmd, workflowId, deduplicationDuration, timeoutMillis),
      s"failed to react to $id with $action",
    )

  override private[admin] def processTransaction(
      tx: Transaction
  )(implicit traceContext: TraceContext): Unit = {
    // Process ping transactions only on the active replica
    if (isActive) {
      val workflowId = WorkflowId(tx.workflowId)
      processPings(DecodeUtil.decodeAllCreated(M.Ping)(tx), workflowId)
      processPongs(DecodeUtil.decodeAllCreated(M.Pong)(tx), workflowId)
      processExplodes(DecodeUtil.decodeAllCreated(M.Explode)(tx), workflowId)
      processMerges(DecodeUtil.decodeAllCreated(M.Merge)(tx))
      processCollapses(DecodeUtil.decodeAllCreated(M.Collapse)(tx), workflowId)
      processProposals(DecodeUtil.decodeAllCreated(M.PingProposal)(tx), workflowId)
    }
  }

  private def duplicateCheck(pingId: String, uniqueId: String, contract: Any)(implicit
      traceContext: TraceContext
  ): Unit = {
    val key = DuplicateIdx(pingId, uniqueId)
    // store contract for later check
    duplicate.get(key) match {
      case None => duplicate += key -> (_ => contract.toString)
      case Some(other) =>
        logger.error(s"Duplicate contract observed for ping-id $pingId: $contract vs $other")
    }
  }

  protected def processProposalsF(
      proposals: Seq[Contract[M.PingProposal]],
      workflowId: WorkflowId,
      unknownInformeesLogLevel: Level = Level.WARN,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    // accept proposals where i'm the next candidate
    proposals.filter(_.value.candidates.headOption.contains(adminParty)).parTraverse_ { proposal =>
      logger.debug(s"Accepting ping proposal ${proposal.value.id} from ${proposal.value.initiator}")
      val command = proposal.contractId.exerciseAccept(adminParty).command
      val id = proposal.value.id
      val action = "ping-proposal-accept"
      submitIgnoringErrors(
        id,
        action,
        command,
        Some(workflowId),
        NoCommandDeduplicationNeeded,
        unknownInformeesLogLevel = unknownInformeesLogLevel,
      ).thereafter {
        case Failure(exc) =>
          logger.error(s"failed to react to $id with $action in $workflowId: $exc")
        case _ =>
      }
    }
  }

  protected def processProposals(proposals: Seq[Contract[M.PingProposal]], workflowId: WorkflowId)(
      implicit traceContext: TraceContext
  ): Unit =
    // We discard the Future without logging, because the exceptions are already logged within `processProposalsF()`
    processProposalsF(proposals, workflowId).discard

  protected def processExplodes(explodes: Seq[Contract[M.Explode]], workflowId: WorkflowId)(implicit
      traceContext: TraceContext
  ): Unit =
    explodes.filter(_.value.responders.contains(adminParty)).foreach { p =>
      duplicateCheck(p.value.id, "explode" + p.value.path, p)
      logger
        .debug(s"$adminParty processing explode of id ${p.value.id} with path ${p.value.path}")
      submitAsync(
        p.value.id,
        "explode" + p.value.path,
        p.contractId.exerciseProcessExplode(adminParty).command,
        Some(workflowId),
        NoCommandDeduplicationNeeded,
      )
    }

  protected def processMerges(
      contracts: Seq[Contract[M.Merge]]
  )(implicit traceContext: TraceContext): Unit = {
    contracts
      .filter(_.value.responders.contains(adminParty))
      .foreach { p =>
        duplicateCheck(p.value.id, "merge" + p.value.path, p)
        logger.debug(s"$adminParty storing merge of ${p.value.id} with path ${p.value.path}")
        merges += MergeIdx(p.value.id, p.value.path) -> MergeItem(p, None)
      }
  }

  protected def processCollapses(contracts: Seq[Contract[M.Collapse]], workflowId: WorkflowId)(
      implicit traceContext: TraceContext
  ): Unit = {

    def addOrCompleteCollapse(
        index: MergeIdx,
        item: PingService.this.MergeItem,
        contract: Contract[M.Collapse],
    ): Unit = {
      val id = contract.value.id
      val path = contract.value.path
      item.first match {
        case None =>
          logger.debug(s"$adminParty observed first collapsed for id $id and path $path")
          merges.update(index, item.copy(first = Some(contract)))
        case Some(other) =>
          logger.debug(
            s"$adminParty observed second collapsed for id $id and path $path. Collapsing."
          )
          merges.remove(index).discard
          // We intentionally don't return the future here, as we just submit the command here and do timeout tracking
          // explicitly with the timeout scheduler.
          submitAsync(
            item.merge.value.id,
            s"collapse-${item.merge.value.path}",
            item.merge.contractId
              .exerciseProcessMerge(adminParty, other.contractId, contract.contractId)
              .command,
            Some(workflowId),
            NoCommandDeduplicationNeeded,
          )
      }
    }

    contracts
      .filter(_.value.responders.contains(adminParty))
      .foreach(p => {
        val index = MergeIdx(p.value.id, p.value.path)
        merges.get(index) match {
          case None => logger.error(s"Received collapse for processed merge: $p")
          case Some(item) => addOrCompleteCollapse(index, item, p)
        }
      })
  }

  private def processPings(pings: Seq[Contract[M.Ping]], workflowId: WorkflowId)(implicit
      traceContext: TraceContext
  ): Unit = {
    def processPing(p: Contract[M.Ping]): Unit = {
      logger.info(s"$adminParty responding to a ping from ${P.Party.unwrap(p.value.initiator)}")
      submitAsync(
        p.value.id,
        "respond",
        p.contractId.exerciseRespond(adminParty).command,
        Some(workflowId),
        NoCommandDeduplicationNeeded,
      )
      scheduleGarbageCollection(p.value.id)
    }
    pings.filter(_.value.responders.contains(adminParty)).foreach(processPing)
  }

  private def scheduleGarbageCollection(id: String)(implicit traceContext: TraceContext): Unit = {
    // remove items from duplicate filter
    val scheduled = clock.scheduleAfter(
      _ => {
        val filteredDuplicates = duplicate.filter(x => x._1.pingId != id)
        if (filteredDuplicates.size != duplicate.size) {
          logger.debug(
            s"Garbage collecting ${duplicate.size - filteredDuplicates.size} elements from duplicate filter"
          )
        }
        duplicate.clear()
        duplicate ++= filteredDuplicates

        val filteredMerges = merges.filter(x => x._1.pingId != id)
        // TODO(#11182): properly clean up stray contracts
        merges.clear()
        merges ++= filteredMerges
      },
      Duration.ofSeconds(1200),
    )
    FutureUtil.doNotAwait(scheduled.unwrap, "failed to schedule garbage collection")
  }

  @VisibleForTesting
  private[admin] def processPongsF(
      pongs: Seq[Contract[M.Pong]],
      workflowId: WorkflowId,
      unknownInformeesLogLevel: Level = Level.WARN,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    pongs.filter(_.value.initiator == adminParty).parTraverse_ { p =>
      // purge duplicate checker
      duplicate.clear()
      // first, ack the pong
      val responder = P.Party.unwrap(p.value.responder)
      logger.info(s"$adminParty received pong from $responder")
      (for {
        _ <- submitIgnoringErrors(
          p.value.id,
          "ack",
          p.contractId.exerciseAck().command,
          Some(workflowId),
          NoCommandDeduplicationNeeded,
          unknownInformeesLogLevel = unknownInformeesLogLevel,
        )
      } yield {
        val id = p.value.id
        responses.get(id) match {
          case None =>
            logger.debug(s"Received response for un-expected ping $id from $responder")
          // receive first (and only correct) pong (we don't now sender yet)
          case Some((None, Some(promise))) =>
            val newPromise = Promise[String]()
            responses.update(id, (Some(responder), Some(newPromise)))
            logger.debug(s"Notifying user about success of ping $id")
            promise.success(responder)
          // receive subsequent pong () which means that we have a duplicate spent!
          case Some((Some(first), Some(promise))) =>
            logger.error(
              s"Received duplicate response for $id from $responder, already received from $first"
            )
            // update responses so we don't fire futures twice even if there are more subsequent pongs
            responses.update(id, (Some(first), None))
            // it's not a success, but the future succeeds
            promise.success(responder)
          // receive even more pongs
          case Some((Some(first), None)) =>
            logger.error(
              s"Received even more responses for $id from $responder, already received from $first"
            )
          // error
          case Some(_) =>
            logger.error(s"Invalid state observed! ${responses.get(id)}")
        }
      }).thereafter {
        case Failure(exc) =>
          logger.error(s"failed to process pong for ${p.value.id} in $workflowId: $exc")
        case _ =>
      }
    }

  private[admin] def processPongs(pongs: Seq[Contract[M.Pong]], workflowId: WorkflowId)(implicit
      traceContext: TraceContext
  ): Unit =
    // We discard the Future without logging, because the exceptions are already logged within `processPongsF()`
    processPongsF(pongs, workflowId).discard

  /** Races the supplied future against a timeout.
    * If the supplied promise completes first the returned future will be completed with this value.
    * If the timeout completes first the returned Future will fail with a [[PingService.TimeoutException]].
    * If a timeout occurs no attempt is made to cancel/stop the provided future.
    */
  private def timeout[T](other: Future[T], durationMillis: Long)(implicit
      traceContext: TraceContext
  ): Future[T] = {
    val result = Promise[T]()

    other.onComplete(result.tryComplete)
    // schedule completing the future exceptionally if it didn't finish before the timeout deadline
    timeoutScheduler.schedule(
      { () =>
        {
          if (result.tryFailure(new TimeoutException))
            logger.info(s"Operation timed out after $durationMillis millis.")
        }
      }: Runnable,
      durationMillis,
      TimeUnit.MILLISECONDS,
    )

    result.future
  }

  private class TimeoutException extends RuntimeException("Future has timed out")
}

object PingService {

  sealed trait Result
  final case class Success(roundTripTime: Duration, responder: String) extends Result
  object Failure extends Result

}
