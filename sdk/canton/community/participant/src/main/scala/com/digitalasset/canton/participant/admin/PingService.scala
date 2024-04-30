// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.implicits.{toBifunctorOps, toFunctorFilterOps}
import com.daml.error.utils.DecodedCantonError
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod.DeduplicationDuration
import com.daml.ledger.api.v2.event.Event.Event
import com.daml.ledger.api.v2.event.CreatedEvent as ScalaCreatedEvent
import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.ledger.api.v2.state_service.ActiveContract
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.daml.ledger.javaapi.data.codegen.ContractId
import com.daml.ledger.javaapi.data.{Command, CreatedEvent as JavaCreatedEvent, Identifier}
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.WorkflowId
import com.digitalasset.canton.ledger.client.{LedgerClient, LedgerClientUtils}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  HasCloseContext,
  Lifecycle,
  PromiseUnlessShutdownFactory,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PingService.{
  AdditionalRetryOnKnownRaceConditions,
  SyncServiceHandle,
}
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.participant.ledger.api.client.{
  CommandResult,
  CommandSubmitterWithRetry,
  LedgerConnection,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.TopologyErrors
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.{DomainId, PartyId}
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.util.{FutureUtil, LoggerUtil}
import com.google.rpc.status.Status
import io.opentelemetry.api.trace.Tracer
import org.slf4j.event.Level
import scalaz.Tag

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.concurrent.*
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.JavaDurationOps

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
    connection: LedgerClient,
    override protected val adminPartyId: PartyId,
    override protected val maxLevelSupported: NonNegativeInt,
    retries: Boolean,
    override protected val maxBongDuration: NonNegativeFiniteDuration,
    override protected val pingResponseTimeout: NonNegativeFiniteDuration,
    override protected val timeouts: ProcessingTimeout,
    override protected val pingDeduplicationDuration: NonNegativeFiniteDuration,
    override protected implicit val tracer: Tracer,
    syncService: SyncServiceHandle,
    override protected val futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
    override protected val clock: Clock,
)(implicit val ec: ExecutionContext)
    extends PingService.Impl
    with AdminWorkflowService
    with FlagCloseable
    with PromiseUnlessShutdownFactory
    with HasCloseContext
    with NamedLogging
    with Spanning {

  override protected def isActive: Boolean = syncService.isActive
  // Execute vacuuming task when (re)connecting to a new domain
  syncService.subscribeToConnections(_.withTraceContext { implicit traceContext => domainId =>
    logger.debug(s"Received connection notification from $domainId")
    vacuumStaleContracts(domainId)
  })

  private def applicationId = "PingService"

  override def onClosed(): Unit = {
    // Note that we can not time out pings nicely here on shutdown as the admin
    // server is closed first, which means that our ping requests will never
    // return proper on shutdown abort
    Lifecycle.close(retrySubmitter, connection)(logger)
  }

  private val retrySubmitter = new CommandSubmitterWithRetry(
    connection.v2.commandService,
    clock,
    futureSupervisor,
    timeouts,
    loggerFactory,
    decideRetry,
  )

  private def decideRetry: Status => Option[FiniteDuration] = status =>
    if (isActive && retries) {
      LedgerClientUtils.defaultRetryRules(status).orElse {
        DecodedCantonError.fromGrpcStatus(status).toOption.collect {
          case decodedError
              if AdditionalRetryOnKnownRaceConditions.contains(decodedError.code.id) =>
            PingService.DefaultRetryableDelay
        }
      }
    } else None

  override protected def submitRetryingOnErrors(
      id: String,
      action: String,
      cmds: Seq[Command],
      domainId: Option[DomainId],
      workflowId: Option[WorkflowId],
      deduplicationDuration: NonNegativeFiniteDuration,
      timeout: NonNegativeFiniteDuration,
  )(implicit traceContext: TraceContext): Future[CommandResult] = {
    // Include a random UUID in the command id, as id/action are not necessarily unique.
    // - The same participant may submit several pings with the same ID.
    // - A ping may get duplicated by CommandSubmitterWithRetry
    val commandId = s"$id-$action-${UUID.randomUUID()}"
    retrySubmitter.submitCommands(
      Commands(
        workflowId = workflowId.map(Tag.unwrap).getOrElse(""),
        applicationId = applicationId,
        commandId = commandId,
        actAs = Seq(adminPartyId.toProtoPrimitive),
        commands = cmds.map(LedgerClientUtils.javaCodegenToScalaProto),
        deduplicationPeriod = DeduplicationDuration(deduplicationDuration.toProtoPrimitive),
        domainId = domainId.map(_.toProtoPrimitive).getOrElse(""),
      ),
      timeout.duration.toScala,
    )
  }

}

object PingService {

  trait SyncServiceHandle {
    def isActive: Boolean
    def subscribeToConnections(subscriber: Traced[DomainId] => Unit): Unit
  }

  private val DefaultRetryableDelay = 1.second
  private val AdditionalRetryOnKnownRaceConditions = Seq(
    TopologyErrors.UnknownInformees,
    TopologyErrors.NoDomainForSubmission,
    TopologyErrors.NoDomainOnWhichAllSubmittersCanSubmit,
    TopologyErrors.InformeesNotActive,
    TopologyErrors.NoCommonDomain,
    TopologyErrors.UnknownContractDomains, // required for restart tests
    RequestValidationErrors.NotFound.Package,
  ).map(_.id)

  /** Cleanup time: when will we deregister pings after their completion */
  private val CleanupPingsTime = NonNegativeFiniteDuration.tryOfSeconds(5)

  /** The command deduplication time for commands that don't need deduplication because
    * any repeated submission would fail anyway, say because the command exercises a consuming choice on
    * a specific contract ID (not contract key).
    */
  private def NoCommandDeduplicationNeeded: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryOfMillis(1)

  sealed trait Result
  final case class Success(roundTripTime: Duration, responder: String) extends Result
  final case class Failure(reason: String) extends Result

  private[admin] final case class TxContext(
      domainId: DomainId,
      workflowId: WorkflowId,
      effectiveAt: CantonTimestamp,
  )

  private[admin] trait Impl {

    this: AdminWorkflowService & NamedLogging & FlagCloseable & PromiseUnlessShutdownFactory & Spanning =>

    protected def adminPartyId: PartyId
    protected def maxLevelSupported: NonNegativeInt
    protected def clock: Clock
    protected def pingDeduplicationDuration: NonNegativeFiniteDuration
    protected def maxBongDuration: NonNegativeFiniteDuration
    protected def pingResponseTimeout: NonNegativeFiniteDuration
    protected implicit def ec: ExecutionContext
    protected def isActive: Boolean
    protected def futureSupervisor: FutureSupervisor
    protected implicit def tracer: Tracer

    override private[admin] def filters: TransactionFilter = {
      // we can't filter by template id as we don't know when the admin workflow package is loaded
      LedgerConnection.transactionFilterByPartyV2(Map(adminPartyId -> Seq.empty))
    }

    private[admin] abstract class ContractWithExpiry(
        val contractId: ContractId[?],
        val template: Identifier,
        initialDomainId: DomainId,
        val workflowId: WorkflowId,
        val expire: CantonTimestamp,
    ) extends PrettyPrinting {

      protected def prettyData: String

      protected val activeSubmission = new AtomicBoolean(false)

      // on creation, we assume that the reassignment counter is 0 as we are anyway only interested in the
      // most recently known location
      protected val currentDomain = new AtomicReference[(DomainId, Long)]((initialDomainId, 0))

      override def pretty: Pretty[ContractWithExpiry] = prettyOfClass(
        param("coid", x => x.contractId.contractId.readableHash),
        param("data", _.prettyData.singleQuoted),
        param(
          "template",
          x =>
            s"${x.template.getModuleName}.${x.template.getEntityName}@${x.template.getPackageId
                .take(8)}...".singleQuoted,
        ),
        param("expire", _.expire),
        param("domainId", _.domainId),
      )

      def domainId: DomainId = currentDomain.get()._1

      def updateDomainId(newDomainId: DomainId, counter: Long): Unit = {
        currentDomain.updateAndGet { case (currentDomainId, currentCounter) =>
          if (counter > currentCounter)
            (newDomainId, counter)
          else
            (currentDomainId, currentCounter)
        }.discard
      }

      def active: Boolean = acs.contains(contractId.contractId)

      /** Initiate response */
      def respond(): Unit

      /** Initiate vaccuming */
      def vacuum(): Unit

      protected def submitVacuum(id: String, cmds: Seq[Command])(implicit
          traceContext: TraceContext
      ): Unit = {
        if (active && !activeSubmission.get()) {
          logger.info(s"$adminPartyId vacuuming ${template.getEntityName} ${prettyData}")
          submitInBackground(
            id,
            s"vacuum-${template.getEntityName}-${prettyData}",
            cmds,
            workflowId,
            activeSubmission,
            clock.now + pingResponseTimeout,
          )
        }
      }

      /** Invoked when archived */
      def archived(): Unit = ()

    }

    private val acs = TrieMap[ContractIdS, ContractWithExpiry]()
    private val directEc = DirectExecutionContext(logger)

    override private[admin] def processTransaction(scalaTx: Transaction): Unit = {
      implicit val traceContext: TraceContext =
        LedgerClient.traceContextFromLedgerApi(scalaTx.traceContext)
      val workflowId = WorkflowId(scalaTx.workflowId)

      val res = for {
        domainId <- DomainId.fromProtoPrimitive(scalaTx.domainId, "domainId")
        effectiveP <- ProtoConverter.required("effectiveAt", scalaTx.effectiveAt)
        effective <- CantonTimestamp.fromProtoTimestamp(effectiveP)
      } yield {
        // process archived
        processArchivedEvents(
          scalaTx.events.map(_.event).collect { case Event.Archived(value) =>
            value.contractId
          }
        )
        // process created
        val context = TxContext(domainId, workflowId, effective)
        processCreatedEvents(scalaTx.events.map(_.event).collect { case Event.Created(value) =>
          (value, context)
        })
      }
      res match {
        case Right(()) => ()
        case Left(err) =>
          logger.error(s"Failed to process transaction ${scalaTx} due to $err")
      }
    }

    private[admin] def processArchivedEvents(
        coids: Seq[String]
    )(implicit traceContext: TraceContext): Unit = {
      val archived = coids.mapFilter { key =>
        acs.get(key) match {
          case Some(item) =>
            logger.info(s"Archived ${item}")
            item.archived()
            Some(item)
          case None =>
            // can happen if somebody starts using the admin party
            None
        }
      }
      if (archived.nonEmpty) {
        FutureUtil.doNotAwait(
          clock
            .scheduleAfter(
              _ => {
                // slowly remove contracts from acs such that we would detect duplicate responses
                archived.foreach(contract => acs.remove(contract.contractId.contractId).discard)
              },
              CleanupPingsTime.duration,
            )
            .onShutdown(()),
          "remove acs",
        )
      }
    }

    private def processCreatedEvents(
        events: Seq[(ScalaCreatedEvent, TxContext)]
    )(implicit
        traceContext: TraceContext
    ): Unit = if (events.nonEmpty) {
      // generate created contracts
      val created = events
        .map { case (event, context) =>
          val javaEvent = JavaCreatedEvent.fromProto(ScalaCreatedEvent.toJavaProto(event))
          (javaEvent.getTemplateId, javaEvent, context)
        }
        .flatMap {
          case (M.ping.Ping.COMPANION.TEMPLATE_ID, event, context) =>
            Seq(pingCreated(context, M.ping.Ping.COMPANION.fromCreatedEvent(event)))
          case (M.bong.BongProposal.COMPANION.TEMPLATE_ID, event, context) =>
            Seq(bongProposalCreated(context, M.bong.BongProposal.COMPANION.fromCreatedEvent(event)))
          case (M.bong.Explode.COMPANION.TEMPLATE_ID, event, context) =>
            Seq(explodeCreated(context, M.bong.Explode.COMPANION.fromCreatedEvent(event)))
          case (M.bong.Collapse.COMPANION.TEMPLATE_ID, event, context) =>
            Seq(collapseCreated(context, M.bong.Collapse.COMPANION.fromCreatedEvent(event)))
          case (M.bong.Merge.COMPANION.TEMPLATE_ID, event, context) =>
            Seq(mergeCreated(context, M.bong.Merge.COMPANION.fromCreatedEvent(event)))
          case (M.bong.Bong.COMPANION.TEMPLATE_ID, event, context) =>
            Seq(bongCreated(context, M.bong.Bong.COMPANION.fromCreatedEvent(event)))
          case _ => Seq.empty
        }
      processCreatedContracts(created)
    }

    private[admin] def processCreatedContracts(created: Seq[ContractWithExpiry])(implicit
        traceContext: TraceContext
    ): Unit = {
      // add them to the acs
      val now = clock.now
      created.foreach { contract =>
        acs.put(contract.contractId.contractId, contract) match {
          case Some(_) => logger.error(s"Duplicate contract ${contract} observed!")
          case None => logger.info(s"Observed create of ${contract}")
        }
        // respond if we are the active instance
        if (isActive) {
          if (contract.expire > now) {
            contract.respond()
          } else {
            contract.vacuum()
          }
        }
      }
      // schedule the regular cleanup of expired contracts. we'll do it once per transaction
      val cleanup = created.foldLeft(clock.now.plus(CleanupPingsTime.duration)) {
        case (cur, next) => cur.max(next.expire)
      }
      FutureUtil.doNotAwait(
        clock
          .scheduleAt(
            _ => {
              // vacuum created contracts
              created.foreach(_.vacuum())
            },
            cleanup.plus(CleanupPingsTime.duration),
          )
          .onShutdown(()),
        "failed to vacuum",
      )
    }

    /** Process a reassignment */
    override private[admin] def processReassignment(tx: Reassignment): Unit = {
      implicit val traceContext: TraceContext =
        LedgerClient.traceContextFromLedgerApi(tx.traceContext)
      tx.event match {
        case Reassignment.Event.UnassignedEvent(value) =>
        // we only look at assign events
        case Reassignment.Event.AssignedEvent(event) =>
          val process = for {
            target <- DomainId.fromProtoPrimitive(event.target, "target")
            created <- ProtoConverter.required("createdEvent", event.createdEvent)
            createdAt <- ProtoConverter.parseRequired(
              CantonTimestamp.fromProtoTimestamp,
              "createdEvent.createdAt",
              created.createdAt,
            )
          } yield {
            acs.get(created.contractId) match {
              case Some(value) => value.updateDomainId(target, event.reassignmentCounter)
              case None => // haven't seen this contract yet, we need to create it
                processCreatedEvents(Seq((created, TxContext(target, WorkflowId(""), createdAt))))
            }
          }
          process.left.foreach { err =>
            logger.error(s"Failed to process reassignment: ${err} / $event")
          }

        case Reassignment.Event.Empty =>
      }
    }

    /** Process the initial active contract set for this service */
    override private[admin] def processAcs(acs: Seq[ActiveContract])(implicit
        traceContext: TraceContext
    ): Unit = {
      val wf = WorkflowId("")
      val loaded = acs.mapFilter { event =>
        val parsed = for {
          domainId <- DomainId.fromProtoPrimitive(event.domainId, "domain_id").leftMap(_.toString)
          createEvent <- event.createdEvent.toRight(s"Empty created event for ${event}???")
          createdAt <- ProtoConverter
            .parseRequired(
              CantonTimestamp.fromProtoTimestamp,
              "createdAt",
              createEvent.createdAt,
            )
            .leftMap(_.toString)
        } yield {
          (createEvent, TxContext(domainId, wf, createdAt))
        }
        parsed match {
          case Right(value) => Some(value)
          case Left(value) =>
            logger.error(s"Unable to parse event ${event}: $value")
            None

        }
      }
      processCreatedEvents(loaded)
    }

    protected def submitRetryingOnErrors(
        id: String,
        action: String,
        cmds: Seq[Command],
        domainId: Option[DomainId],
        workflowId: Option[WorkflowId],
        deduplicationDuration: NonNegativeFiniteDuration,
        timeout: NonNegativeFiniteDuration,
    )(implicit traceContext: TraceContext): Future[CommandResult]

    protected def submitInBackground(
        id: String,
        action: String,
        cmds: Seq[Command],
        workflowId: WorkflowId,
        flag: AtomicBoolean,
        expire: CantonTimestamp,
    )(implicit traceContext: TraceContext): Unit = {
      NonNegativeFiniteDuration.create(expire - clock.now) match {
        case Right(timeout) =>
          if (!flag.getAndSet(true)) {
            superviseBackgroundSubmission(
              action,
              timeout,
              withSpan(s"PingService.$action") { implicit traceContext => _span =>
                submitRetryingOnErrors(
                  id,
                  action,
                  cmds,
                  None,
                  Some(workflowId),
                  NoCommandDeduplicationNeeded,
                  timeout,
                ).thereafter { _ =>
                  flag.set(false)
                }
              },
            )
          } else {
            logger.debug(
              s"Skipping background submission of ${action} as one is already in progress"
            )
          }
        case Left(err) =>
          logger.debug("Not submitting background submission as it is already expired: " + err)
      }
    }

    protected def superviseBackgroundSubmission(
        description: String,
        timeout: NonNegativeFiniteDuration,
        submission: Future[CommandResult],
    )(implicit traceContext: TraceContext): Unit = {
      futureSupervisor
        .supervised(description, timeout.duration.toScala.plus(1.second))(submission)
        .foreach {
          case CommandResult.Success(_) =>
            logger.debug(s"Successfully submitted ${description}")
          case CommandResult.Failed(_, errorStatus) =>
            logger.info(s"Submission ${description} failed with ${errorStatus}")
          case CommandResult.AbortedDueToShutdown =>
            logger.info(s"Submission ${description} was aborted due to shutdown")
          case CommandResult.TimeoutReached(_, lastErrorStatus) =>
            logger.info(
              s"Submission ${description} was aborted due to timeout with last status ${lastErrorStatus}"
            )
        }
    }

    private[admin] type PingId = String
    private[admin] type ContractIdS = String

    private val requests: TrieMap[PingId, PingRequest] = new TrieMap()

    /** Send a ping to the target party, return round-trip time or a timeout
      *
      * @param targetParties String     the parties to send ping to
      * @param validators    additional validators (signatories) of the contracts
      * @param timeout  how long to wait for pong
      * @param domainId      the domain to send the ping to
      */
    def ping(
        targetParties: Set[PartyId],
        validators: Set[PartyId],
        timeout: NonNegativeFiniteDuration,
        maxLevel: NonNegativeInt = NonNegativeInt.tryCreate(0),
        domainId: Option[DomainId] = None,
        workflowId: Option[WorkflowId] = None,
        id: String = UUID.randomUUID().toString,
    )(implicit traceContext: TraceContext): Future[PingService.Result] = {
      def reject(reason: String): Future[PingService.Result] = {
        Future.successful(Failure(reason))
      }
      if (isClosing) {
        reject("Aborting ping due to shutdown")
      } else if (maxLevel > maxLevelSupported) {
        reject(s"Max level ${maxLevel} exceeds supported max level ${maxLevelSupported}")
      } else if (targetParties.isEmpty) {
        reject("No target parties specified for ping")
      } else {
        val request = PingRequest(
          id = id,
          started = clock.now,
          timeout = timeout,
          targetParties = targetParties,
          validators = validators,
          maxLevel = maxLevel,
          domainId = domainId,
          workflowId = workflowId,
        )
        requests.putIfAbsent(id, request) match {
          case None =>
            // schedule ping timeout in case we don't receive any response
            FutureUtil.doNotAwait(
              clock
                .scheduleAfter(request.pingTimedout, timeout.duration)
                .onShutdown {
                  // normally, the admin server is shutdown before the ping services
                  // this is to avoid that any new command arrives while we are shutting down.
                  // unfortunately, this means we can't expire the ping requests nicely.
                  // there is a trick though: the clock is also shutdown before the admin server,
                  // so if we just react on the shutdown event there, we can terminate the pings
                  request.promise.trySuccess {
                    PingService.Failure("Aborting ping due to shutdown")
                  }.discard
                  ()
                },
              "cleaning up the request",
            )
            request.submit()
            request.promise.future
          case Some(_) => reject(s"Duplicate ping request $id")
        }
      }
    }

    /** A ping request
      *
      * @param id          identifier of the ping request
      * @param promise     the promise to be fulfilled when the ping is complete
      */
    private case class PingRequest(
        id: PingId,
        started: CantonTimestamp,
        timeout: NonNegativeFiniteDuration,
        targetParties: Set[PartyId],
        validators: Set[PartyId],
        maxLevel: NonNegativeInt,
        domainId: Option[DomainId],
        workflowId: Option[WorkflowId],
    )(implicit val traceContext: TraceContext)
        extends PrettyPrinting {

      /** The promise which will be fulfilled once the ping completes
        *
        * We don't use FutureUnlessShutdown as we control the shutdown manually and use
        * a trick to cancel the pings before the admin server shuts down
        */
      val promise: Promise[PingService.Result] = Promise[PingService.Result]()

      def observed(): Unit = {
        logger.info("Observed creation of ping contract, waiting for archival by responder")
        observedPing.set(true)
      }

      private val observedPing = new AtomicBoolean(false)

      def receivedResponse(respondedBy: String): Unit = {
        val duration = clock.now - started
        if (!promise.isCompleted) {
          logger.info(
            s"Observed archival of ping contract after ${LoggerUtil.roundDurationForHumans(duration.toScala)}"
          )
          promise
            .trySuccess(PingService.Success(duration, respondedBy))
            .discard

        } else {
          logger.info(
            s"Observed archival of expired ping contract after ${LoggerUtil.roundDurationForHumans(duration.toScala)}"
          )
        }
      }

      def pingTimedout(now: CantonTimestamp): Unit = {
        // no need to schedule vacuuming here, as this is scheduled as part of the create event
        requests.remove(id).foreach { _ =>
          if (promise.isCompleted) {
            if (!isClosing) {
              logger.error(
                "Trying to timeout ping, but it seems to be completed already despite us not being closed?"
              )
            }
          } else {
            val reason =
              if (observedPing.get())
                "We were able to create the ping contract, but responder did not respond in time"
              else "We were unable to create the ping contract"
            logger.info(
              s"Ping $id timeout ($reason) out after ${LoggerUtil.roundDurationForHumans((now - started).toScala)}"
            )
            promise.trySuccess(Failure(s"Timeout: $reason")).discard
          }
        }
      }

      protected def reportFailure(reason: String, level: Level): Unit = {
        val str = s"Failed ping id=${id}: ${reason}"
        LoggerUtil.logAtLevel(level, str)
        requests
          .remove(id)
          .foreach(_.promise.trySuccess(Failure(str)))
      }

      override def pretty: Pretty[PingRequest] = prettyOfClass(
        param("id", _.id.singleQuoted),
        paramIfNonEmpty("domainId", _.domainId),
        paramIfNonEmpty("workflowId", _.workflowId.map(Tag.unwrap(_).singleQuoted)),
        param("target", _.targetParties),
        param("timeout", _.timeout),
        paramIfNonEmpty("validators", _.validators),
        paramIfNotDefault("maxLevel", _.maxLevel.value, 0),
      )

      def submit(): Unit = {
        val (name, command) =
          if (validators.isEmpty && targetParties.size == 1 && maxLevel.value == 0) {
            logger.info(show"Starting ping ${this}")
            (
              "ping",
              new M.ping.Ping(
                id,
                adminPartyId.toProtoPrimitive,
                targetParties.headOption
                  .getOrElse(adminPartyId)
                  .toProtoPrimitive, // party exists for sure
              ),
            )
          } else {
            logger.info(show"Starting bong-proposal ${this}")
            val candidates = validators.map(_.toProtoPrimitive).toSeq.asJava
            (
              "bong-proposal",
              new M.bong.BongProposal(
                id,
                adminPartyId.toProtoPrimitive,
                if (candidates.isEmpty) Seq(adminPartyId.toProtoPrimitive).asJava else candidates,
                List.empty.asJava,
                targetParties.map(_.toProtoPrimitive).toSeq.asJava,
                maxLevel.value,
                started.plus(timeout.duration).toInstant,
              ),
            )
          }
        def handleCommandResult(result: CommandResult): Unit = result match {
          case CommandResult.Success(transactionId) =>
            logger.info(
              s"Successfully submitted ping ${id} with transactionId=${transactionId}, waiting for response"
            )
          case CommandResult.Failed(_, errorStatus) =>
            // warning as we failed premature
            reportFailure(s"Failed to submit ping ${id}: ${errorStatus}", Level.WARN)
          case CommandResult.AbortedDueToShutdown =>
            reportFailure(s"Ping ${id} aborted due to shutdown", Level.INFO)
          case CommandResult.TimeoutReached(_, lastErrorStatus) =>
            reportFailure(
              s"Timeout out while attempting to submit ping ${id}: ${lastErrorStatus}",
              Level.INFO,
            )
        }
        withSpan("PingService.submit") { implicit traceContext => _span =>
          submitRetryingOnErrors(
            id,
            name,
            command.create.commands.asScala.toSeq,
            domainId,
            workflowId,
            pingDeduplicationDuration,
            timeout,
          )
        }.onComplete {
          case scala.util.Success(result) => handleCommandResult(result)
          case scala.util.Failure(exception) =>
            logger.error(
              s"Ping submission ${this} failed unexpectedly with an exception",
              exception,
            )
            requests
              .remove(id)
              .foreach(
                _.promise
                  .trySuccess(Failure("Internal error due to exception"))
              )
        }(directEc) // parasitic to avoid shutdown issues

      }

    }

    private[admin] def pingCreated(
        context: TxContext,
        ping: M.ping.Ping.Contract,
    )(implicit
        traceContext: TraceContext
    ): ContractWithExpiry = {

      // determine expiry (take timeout if this our own, otherwise use the parameter)
      val timeout = requests.get(ping.data.id) match {
        case Some(ping) =>
          ping.observed()
          ping.timeout
        case None => pingResponseTimeout
      }

      new ContractWithExpiry(
        ping.id,
        ping.getContractTypeId,
        context.domainId,
        context.workflowId,
        // using clock.now as with the ping, it doesn't really make a difference if we vacuum or respond
        expire = clock.now + timeout,
      ) {

        override protected def prettyData: String = ping.data.id

        override def respond(): Unit = {
          if (ping.data.responder == adminPartyId.toProtoPrimitive) {
            logger.info(s"$adminPartyId responding to a ping from ${ping.data.initiator}")
            submitInBackground(
              ping.data.id,
              "ping-response",
              ping.id.exerciseRespond().commands.asScala.toSeq,
              workflowId,
              activeSubmission,
              expire,
            )
          }
        }

        override def vacuum(): Unit = submitVacuum(
          ping.data.id,
          ping.id.exerciseAbortPing(adminPartyId.toProtoPrimitive).commands.asScala.toSeq,
        )

        override def archived(): Unit = completedPing(ping.data.id, ping.data.responder)

      }
    }

    private def toCappedBongExpiry(instant: Instant): CantonTimestamp =
      CantonTimestamp
        .fromInstant(instant)
        .getOrElse(CantonTimestamp.Epoch)
        .min(clock.now + maxBongDuration)

    private[admin] def bongCreated(
        context: TxContext,
        bong: M.bong.Bong.Contract,
    )(implicit
        traceContext: TraceContext
    ): ContractWithExpiry = {
      new ContractWithExpiry(
        bong.id,
        bong.getContractTypeId,
        context.domainId,
        context.workflowId,
        expire = toCappedBongExpiry(bong.data.timeout),
      ) {

        override protected def prettyData: String = bong.data.id

        requests.get(bong.data.id).foreach(_.observed())

        override def respond(): Unit = {
          if (bong.data.initiator == adminPartyId.toProtoPrimitive && !activeSubmission.get()) {
            logger.info(s"$adminPartyId acknowledging completed bong")
            submitInBackground(
              bong.data.id,
              "bong-response",
              bong.id.exerciseAck().commands.asScala.toSeq,
              workflowId,
              activeSubmission,
              expire,
            )
          }
        }

        override def vacuum(): Unit = submitVacuum(
          bong.data.id,
          bong.id.exerciseAbortBong(adminPartyId.toProtoPrimitive).commands.asScala.toSeq,
        )

        override def archived(): Unit =
          completedPing(bong.data.id, bong.data.responder)

      }
    }

    private def completedPing(id: PingId, responder: String): Unit = {
      requests.remove(id) match {
        case Some(request) => request.receivedResponse(responder)
        case None => // can happen if we e.g. restarted and lost a pending ping
      }
    }

    private[admin] def bongProposalCreated(
        context: TxContext,
        proposal: M.bong.BongProposal.Contract,
    )(implicit
        traceContext: TraceContext
    ): ContractWithExpiry = {

      new ContractWithExpiry(
        proposal.id,
        proposal.getContractTypeId,
        context.domainId,
        context.workflowId,
        expire = toCappedBongExpiry(proposal.data.timeout),
      ) {

        override protected def prettyData: String = proposal.data.id

        override def respond(): Unit = {
          if (proposal.data.maxLevel >= maxLevelSupported.value) {
            vacuum()
          } else if (
            proposal.data.candidates.asScala.headOption.contains(adminPartyId.toProtoPrimitive)
          ) {
            logger.info(s"$adminPartyId accepting bong proposal from ${proposal.data.initiator}")
            submitInBackground(
              proposal.data.id,
              "bong-proposal-accept",
              proposal.id.exerciseAccept(adminPartyId.toProtoPrimitive).commands.asScala.toSeq,
              workflowId,
              activeSubmission,
              expire,
            )
          }
        }

        override def vacuum(): Unit = submitVacuum(
          proposal.data.id,
          proposal.id
            .exerciseAbortBongProposal(adminPartyId.toProtoPrimitive)
            .commands
            .asScala
            .toSeq,
        )

        override def archived(): Unit = ()
      }
    }

    private[admin] def explodeCreated(
        context: TxContext,
        contract: M.bong.Explode.Contract,
    )(implicit
        traceContext: TraceContext
    ): ContractWithExpiry = new ContractWithExpiry(
      contract.id,
      contract.getContractTypeId,
      context.domainId,
      context.workflowId,
      expire = toCappedBongExpiry(contract.data.timeout),
    ) {
      override protected def prettyData: String = s"${contract.data.path} of ${contract.data.id}"

      /** Initiate response */
      override def respond(): Unit = if (contract.data.maxLevel >= maxLevelSupported.value)
        vacuum()
      else if (contract.data.responders.contains(adminPartyId.toProtoPrimitive)) {
        logger
          .debug(
            s"$adminPartyId processing explode of id ${contract.data.id} with path ${contract.data.path}"
          )
        submitInBackground(
          contract.data.id,
          "explode" + contract.data.path,
          contract.id.exerciseProcessExplode(adminPartyId.toProtoPrimitive).commands.asScala.toSeq,
          workflowId,
          activeSubmission,
          expire,
        )
      }

      override def vacuum(): Unit = submitVacuum(
        contract.data.id,
        contract.id.exerciseAbortExplode(adminPartyId.toProtoPrimitive).commands.asScala.toSeq,
      )

    }

    private case class MergeIdx(pingId: String, path: String)
    private case class MergeItem(
        merge: M.bong.Merge.Contract,
        first: Option[M.bong.Collapse.Contract],
    )
    private val merges: TrieMap[MergeIdx, MergeItem] = new TrieMap()
    private[admin] def mergeCreated(
        context: TxContext,
        contract: M.bong.Merge.Contract,
    )(implicit
        traceContext: TraceContext
    ): ContractWithExpiry = new ContractWithExpiry(
      contract.id,
      contract.getContractTypeId,
      context.domainId,
      context.workflowId,
      expire = toCappedBongExpiry(contract.data.timeout),
    ) {
      override protected def prettyData: String = s"${contract.data.path} of ${contract.data.id}"
      private lazy val idx = MergeIdx(contract.data.id, contract.data.path)

      override def respond(): Unit = if (maxLevelSupported.value > 0) {
        logger.debug(
          s"$adminPartyId storing merge of ${contract.data.id} with path ${contract.data.path}"
        )
        merges += idx -> MergeItem(contract, None)
      } else {
        vacuum()
      }

      override def vacuum(): Unit = submitVacuum(
        contract.data.id,
        contract.id.exerciseAbortMerge(adminPartyId.toProtoPrimitive).commands.asScala.toSeq,
      )

    }

    private[admin] def collapseCreated(
        context: TxContext,
        contract: M.bong.Collapse.Contract,
    )(implicit
        traceContext: TraceContext
    ): ContractWithExpiry = new ContractWithExpiry(
      contract.id,
      contract.getContractTypeId,
      context.domainId,
      context.workflowId,
      expire = toCappedBongExpiry(contract.data.timeout),
    ) {
      override protected def prettyData: String = s"${contract.data.path} of ${contract.data.id}"
      private lazy val index = MergeIdx(contract.data.id, contract.data.path)

      override def respond(): Unit = if (
        contract.data.responders.contains(
          adminPartyId.toProtoPrimitive
        ) && maxLevelSupported.value > 0
      ) {
        merges.get(index) match {
          case None => logger.error(s"Received collapse for processed merge: $contract")
          case Some(item) =>
            val id = contract.data.id
            val path = contract.data.path
            item.first match {
              case None =>
                logger.debug(s"$adminPartyId observed first collapsed for id $id and path $path")
                merges.update(index, item.copy(first = Some(contract)))
              case Some(other) =>
                logger.debug(
                  s"$adminPartyId observed second collapsed for id $id and path $path. Collapsing."
                )
                merges.remove(index).discard
                // We intentionally don't return the future here, as we just submit the command here and do timeout tracking
                // explicitly with the timeout scheduler.
                submitInBackground(
                  item.merge.data.id,
                  s"collapse-${item.merge.data.path}",
                  item.merge.id
                    .exerciseProcessMerge(adminPartyId.toProtoPrimitive, other.id, contract.id)
                    .commands
                    .asScala
                    .toSeq,
                  workflowId,
                  activeSubmission,
                  expire,
                )
            }
        }
      }

      override def archived(): Unit = merges.remove(index).discard

      override def vacuum(): Unit = submitVacuum(
        contract.data.id,
        contract.id.exerciseAbortCollapse(adminPartyId.toProtoPrimitive).commands.asScala.toSeq,
      )
    }

    protected def vacuumStaleContracts(
        domainId: DomainId
    )(implicit traceContext: TraceContext): Unit = {
      val now = clock.now
      val items = acs.collect {
        case (_, value) if value.domainId == domainId && value.expire < now => value
      }
      if (items.nonEmpty) {
        logger.info(s"Vacuuming ${items.size} stale contracts for ${domainId}")
        items.foreach(_.vacuum())
      }
    }

  }

}
