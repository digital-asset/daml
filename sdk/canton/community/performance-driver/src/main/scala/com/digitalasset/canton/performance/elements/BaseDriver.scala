// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.commands.Command.toJavaProto
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod.Empty
import com.daml.ledger.api.v2.commands.{Command, Commands}
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.event.Event.Event
import com.daml.ledger.api.v2.event.Event.Event.{Archived, Created, Exercised}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.codegen.{Contract, ContractId}
import com.daml.ledger.javaapi.data.{Identifier, Party, Template}
import com.digitalasset.base.error.utils.ErrorDetails.ErrorInfoDetail
import com.digitalasset.base.error.utils.{DecodedCantonError, ErrorDetails}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.{FullClientConfig, ProcessingTimeout}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.{
  UnknownInformees,
  UnknownSubmitters,
}
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.UserId
import com.digitalasset.canton.ledger.client.ResilientLedgerSubscription
import com.digitalasset.canton.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.DuplicateCommand
import com.digitalasset.canton.lifecycle.{FlagCloseable, LifeCycle, SyncCloseable}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ledger.api.client.LedgerConnection
import com.digitalasset.canton.performance.acs.{ContractObserver, ContractStore}
import com.digitalasset.canton.performance.control.{LatencyMonitor, SubmissionRate}
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.{Connectivity, RateSettings}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, NoTracing, TracerProvider}
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.google.protobuf.any.Any
import com.google.rpc.Code
import com.google.rpc.status.Status
import com.typesafe.scalalogging.LazyLogging
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.FlowShape
import org.apache.pekko.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Source}

import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success, Try}

trait DriverControl {

  /** returns true if process is active and we should continue submitting commands */
  def active: Boolean

  /** error handler invoked if fatal error has been observed by any driver */
  def disable(): Unit

}

final case class SubCommand(
    baseDesc: String,
    reference: String,
    command: Command,
    synchronizerId: SynchronizerId,
    pending: Future[Boolean] => Unit,
    failed: () => Unit = () => (),
) {
  val promise = Promise[Boolean]()
  pending(promise.future)
}

/** Shared implementation between master, trader and issuer roles.
  *
  * The base driver contains the shared code between each role. In here, we setup the connection,
  * read the ACS on startup, initialise the driver and provide a set of utilities that all roles
  * will need (such as submitting and tracking submissions or dealing with pending contracts)
  *
  * We also implement here the basic nanobot update logic.
  */
abstract class BaseDriver(
    connectivity: Connectivity,
    partyLf: LfPartyId,
    masterPartyLf: LfPartyId,
    commandClientConfiguration: CommandClientConfiguration,
    val loggerFactory: NamedLoggerFactory,
    control: DriverControl,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends BaseDriver.Flusher[GetUpdatesResponse]
    with NamedLogging
    with FlagCloseable
    with NoTracing
    with HasFlushFuture {

  override protected val timeouts: ProcessingTimeout = ProcessingTimeout()

  private val tracerProvider: TracerProvider = NoReportingTracerProvider
  protected val party = new Party(partyLf: String)
  protected val userId = "PerformanceTest"
  protected val masterParty = new Party(masterPartyLf: String)
  protected val clientConfig =
    FullClientConfig(connectivity.host, connectivity.port, connectivity.tls)
  protected val ledgerClient =
    LedgerConnection.createLedgerClient(
      UserId(userId),
      clientConfig,
      commandClientConfiguration,
      tracerProvider,
      loggerFactory,
      None,
    )

  override def isActive: Boolean = control.active
  protected val listeners = ListBuffer[ContractObserver]()
  protected val pendingCommands = TrieMap[String, Instant]()
  protected val commandIdGen = new AtomicInteger(0)
  protected val currentStatus = new AtomicReference[Option[DriverStatus]](None)
  protected val running = new AtomicBoolean(true)
  protected val done_ = Promise[Unit]()

  protected def finished(): Unit =
    if (running.getAndSet(false)) {
      logger.info("I hereby declare myself to have completed all my processing.")
      val _ = doFlush().map { _ =>
        logger.debug("Finished flushing")
        done_.trySuccess(())
      }
    }

  protected val masterContract =
    new ContractStore[
      M.orchestration.TestRun.Contract,
      M.orchestration.TestRun.ContractId,
      M.orchestration.TestRun,
      Unit,
    ](
      "master test runner",
      M.orchestration.TestRun.COMPANION,
      index = _ => (),
      filter = x => x.data.master == masterParty.getValue,
      loggerFactory,
    ) {
      override protected def contractCreated(
          create: M.orchestration.TestRun.Contract,
          index: Unit,
      ): Unit =
        masterCreated(create)
    }

  listeners.append(masterContract)

  protected def mapCommand(
      fut: Future[Boolean],
      reference: String,
  ): EitherT[Future, String, Unit] =
    EitherT(fut.map(Either.cond(_, (), s"Command failed with reference $reference")))

  protected def masterCreated(create: M.orchestration.TestRun.Contract): Unit = {}

  private val subscription: SingleUseCell[ResilientLedgerSubscription[?, ?]] =
    new SingleUseCell[ResilientLedgerSubscription[?, ?]]()

  protected def subscribeToTemplates: Seq[Identifier]

  def start(): Future[Either[String, Unit]] = {
    logger.info(s"Starting driver for $name")

    val templates = Seq(
      M.orchestration.TestRun.TEMPLATE_ID,
      M.orchestration.TestParticipant.TEMPLATE_ID,
      M.orchestration.TestProbe.TEMPLATE_ID,
      M.orchestration.ParticipationRequest.TEMPLATE_ID,
      M.generator.Generator.TEMPLATE_ID,
    ) ++ subscribeToTemplates

    val format = LedgerConnection.eventFormatByParty(
      Map(
        PartyId.tryFromLfParty(partyLf) ->
          templates.map(LedgerConnection.mapTemplateIds)
      )
    )

    val offsetF: Future[Long] =
      if (connectivity.reprocessAcs) {
        val started = System.nanoTime()
        logger.debug("Starting fetching of active contracts")
        ledgerClient.stateService.getLedgerEndOffset().flatMap { offset =>
          ledgerClient.stateService
            .getActiveContracts(eventFormat = format, validAtOffset = offset, token = None)
            .map { acs =>
              logger.debug(s"Fetching of active contracts took ${LoggerUtil
                  .roundDurationForHumans(Duration.fromNanos(System.nanoTime() - started))}")
              // apply acs to local state
              acs.foreach(active => processCreate(active.getCreatedEvent).discard[Boolean])
              offset
            }
        }
      } else {
        logger.debug("Getting offset from ledgerEnd")
        ledgerClient.stateService
          .getLedgerEndOffset()
      }

    offsetF.map { offset =>
      logger.debug(s"Subscribing as of $offset")
      val myDriver = driver()
      ErrorUtil.requireArgument(subscription.isEmpty, "subscription should not be set twice?")
      subscription
        .putIfAbsent(
          new ResilientLedgerSubscription(
            makeSource = subscribeOffset =>
              ledgerClient.updateService
                .getUpdatesSource(subscribeOffset, format),
            consumingFlow = BaseDriver.buildGraph(myDriver),
            subscriptionName = "PerformanceRunner",
            startOffset = offset,
            extractOffset = ResilientLedgerSubscription.extractOffsetFromGetUpdateResponse,
            timeouts = timeouts,
            loggerFactory = loggerFactory,
          )
        )
        .discard
      Either.unit
    }
  }

  def done(): Future[Unit] = done_.future

  private def processUpdates(transaction: Transaction): Boolean = {
    logger.info(
      s"Observed transaction with commandId=${transaction.commandId} and updateId=${transaction.updateId} and offset=${transaction.offset}"
    )
    if (transaction.commandId.nonEmpty) {
      latencyMonitor.foreach(_.observedTransaction(transaction.commandId))
    }
    transaction.events.toList
      .map(_.event)
      .map {
        case Created(createEvent) => processCreate(createEvent)
        case Archived(archiveEvent) => listeners.map(_.processArchive(archiveEvent)).exists(x => x)
        case Exercised(_) =>
          logger.warn("Exercised event is not expected here")
          false
        case Event.Empty => false
      }
      .exists(x => x)
  }

  private def processCreate(create: CreatedEvent): Boolean =
    listeners.map(_.processCreate(create)).exists(x => x)

  override def onClosed(): Unit =
    LifeCycle.close(
      SyncCloseable("ledger subscription", subscription.get.foreach(_.close())),
      ledgerClient,
    )(logger)

  override def update(updates: List[GetUpdatesResponse]): Boolean =
    updates.map(_.update).foldLeft(false) {
      case (acc, GetUpdatesResponse.Update.Transaction(transaction)) =>
        processUpdates(transaction) || acc
      // NOTE: for multi-synchronizer, we'll have to implement tracking of reassignments
      case (acc, _) => acc
    }

  protected def setPending[
      TC <: Contract[TCid, T],
      TCid <: ContractId[T],
      T <: Template,
      L,
  ](
      cs: ContractStore[TC, TCid, T, L],
      cid: ContractId[T],
      fut: Future[Boolean],
  ): Unit = {
    def sp(b: Boolean): Unit = cs.setPending(cid, isPending = b).discard
    sp(true)
    fut.onComplete {
      case Success(true) => ()
      case Success(false) =>
        logger.debug(s"Command failed making contract $cid not pending")
        sp(false)
      case Failure(_) =>
        logger.info(s"Command exception, setting $cid to not pending")
        sp(false)
    }
  }

  protected def submitBatched(
      commands: Seq[SubCommand],
      batchSize: Int,
      submissionRate: SubmissionRate,
      duplicateSubmissionDelay: => Option[NonNegativeFiniteDuration],
  ): Unit =
    commands.grouped(batchSize).foreach { batch =>
      val submissionF = submitSubCommands(batch, duplicateSubmissionDelay)
      submissionRate.newSubmission(submissionF)
    }

  private def submitSubCommands(
      commands: Seq[SubCommand],
      duplicateSubmissionDelay: => Option[NonNegativeFiniteDuration],
  ): Future[Boolean] =
    commands
      .groupBy(_.synchronizerId)
      .toSeq
      .parTraverse { case (synchronizerId, commands) =>
        val cmds = commands.map(_.command)
        val baseDescription = commands.map(_.baseDesc).mkString("-")
        val reference = "(" + commands.map(_.reference).mkString(",") + ")"
        val fut = submitCommand(
          baseDescription,
          cmds.map(c => javaapi.data.Command.fromProtoCommand(toJavaProto(c))),
          reference,
          () => commands.foreach(_.failed()),
          duplicateSubmissionDelay,
          synchronizerId = Some(synchronizerId),
        )
        // complete the promised futures
        fut.thereafter { res =>
          commands.foreach(_.promise.complete(res))
        }
      }
      .map(_.forall(identity))

  protected def latencyMonitor: Option[LatencyMonitor] = None

  /** submit commands to ledger
    *
    * @param failed
    *   callback to be invoked if the command submission fails
    * @param duplicateSubmissionDelay
    *   duration to wait before submitting the same command (with an extended command-id) twice to
    *   simulate contention.
    */
  protected def submitCommand(
      baseDescription: String,
      commandJava: Seq[javaapi.data.Command],
      reference: String,
      failed: () => Unit = () => (),
      duplicateSubmissionDelay: => Option[NonNegativeFiniteDuration] = None,
      synchronizerId: Option[SynchronizerId] = None,
      fixedCommandId: Option[String] = None,
  ): Future[Boolean] = {
    val current = pendingCommands.size
    val commandCtr = commandIdGen.addAndGet(1)
    val tm = Instant.now
    val suffix = s"-$commandCtr-$name-${tm.getEpochSecond}"
    val commandId = fixedCommandId.getOrElse(baseDescription.take(250 - suffix.length) + suffix)
    val command = commandJava.map(c => Command.fromJavaProto(c.toProtoCommand))
    pendingCommands.put(commandId, tm).discard
    latencyMonitor.foreach(_.schedule(commandId))

    logger.debug(
      s"Submitting command $reference with commandId=$commandId (pending=$current) via ledger api"
    )

    def handleFailed(status: Either[Throwable, Status]): Success[Boolean] = {
      status match {
        case Right(status) if status.code == Code.RESOURCE_EXHAUSTED_VALUE =>
          logger.info(
            s"Backpressure for $reference with commandId=$commandId: ${status.message}"
          )
        case Right(status) if status.code == Code.UNAVAILABLE_VALUE =>
          logger.info(
            s"Aborted due to shutdown $reference with commandId=$commandId"
          )
        case Right(status) if status.code == Code.ALREADY_EXISTS_VALUE =>
          val isDuplicateCommand = ErrorDetails
            .from(anys = status.details.map(Any.toJavaProto))
            .exists {
              case ErrorInfoDetail(DuplicateCommand.id, _) => true
              case _ => false
            }
          if (isDuplicateCommand) {
            logger.info(s"Command submission $reference with commandId=$commandId is a duplicate")
          }
        case Right(status) if status.code == Code.NOT_FOUND_VALUE =>
          val isUnknownInformeesOrSubmitter = ErrorDetails
            .from(anys = status.details.map(Any.toJavaProto))
            .exists {
              case ErrorInfoDetail(UnknownInformees.id | UnknownSubmitters.id, _) => true
              case _ => false
            }
          if (isUnknownInformeesOrSubmitter) {
            logger.info(
              s"Unknown informees or submitter for $reference with commandId=$commandId, possibly because topology state hasn't fully synchronized across all nodes."
            )
          } else {
            logger.error(s"Failed to submit $reference with commandId=$commandId due to $status")
          }
        case Right(status) =>
          DecodedCantonError.fromGrpcStatus(status) match {
            case Right(err) =>
              logger.info(s"Failed to submit $reference with commandId=$commandId due to $err")
            case Left(_) =>
              logger.error(
                s"Failed to submit $reference with commandId=$commandId due to $status"
              )
          }
        case Left(throwable) =>
          logger.error(s"Really failed to submit $reference with commandId=$commandId ", throwable)
      }
      latencyMonitor.foreach(_.failed(commandId))
      failed()
      Success(false)
    }

    def handleSuccess(secondary: Option[Status]): Success[Boolean] = {
      val nw = Instant.now
      val latency = nw.toEpochMilli - tm.toEpochMilli
      val secondaryString = secondary
        .map { status =>
          s", secondary=${Code.forNumber(status.code)}: ${status.message}}"
        }
        .getOrElse("")
      logger.debug(
        s"Successfully submitted $reference with commandId=$commandId  (latency: $latency ms)$secondaryString"
      )
      Success(true)
    }

    def submit(suffix: String) =
      ledgerClient.commandService
        .submitAndWait(
          Commands(
            workflowId = commandId + suffix,
            userId = userId,
            commandId = commandId + suffix,
            commands = command,
            deduplicationPeriod = Empty,
            minLedgerTimeAbs = None,
            minLedgerTimeRel = None,
            actAs = Seq(partyLf),
            readAs = Nil,
            submissionId = "",
            disclosedContracts = Nil,
            synchronizerId = synchronizerId.fold("")(_.toProtoPrimitive),
            packageIdSelectionPreference = Nil,
            prefetchContractKeys = Nil,
          )
        )

    val submitF = submit("")
    val submitDelayedF = duplicateSubmissionDelay.traverse(delay =>
      DelayUtil.delay("delayed-submission", delay.toScala, this).flatMap { _ =>
        submit("race")
      }
    )

    val result = submitF.zip(submitDelayedF).transform {
      // if we only submitted one and it succeeded, we are good
      case Success((Right(_), None)) =>
        handleSuccess(None)
      // if both succeeded, we have a problem
      case Success((Right(_), Some(Right(_)))) =>
        logger.error(s"Both submissions for $commandId succeeded, this should not happen")
        handleSuccess(None)
      // if one succeeded, we are good
      case Success((Right(_), Some(Left(status)))) =>
        handleSuccess(Some(status))
      case Success((Left(status), Some(Right(_)))) =>
        handleSuccess(Some(status))
      // if we only had left and it failed, mark as failed
      case Success((Left(status), None)) =>
        handleFailed(Right(status))
      // if both failed, display both errors
      case Success((Left(status), Some(Left(status2)))) =>
        logger.debug(s"Note, secondary submission failed as well with $status2")
        handleFailed(Right(status))
      case Failure(ex: Throwable) =>
        handleFailed(Left(ex))
    }

    addToFlushWithoutLogging(s"command $reference")(
      result
    ) // add to flush so we don't terminate this process before all commands have finished

    result.onComplete(_ => pendingCommands.remove(commandId))
    result
  }

  def status(): Option[DriverStatus] = currentStatus.get()

  def updateRateSettings(update: RateSettings => RateSettings): Unit = {}

}

/** flush state used to control when the driver decides to send commands once he knows there are no
  * further updates to be consumed
  */
sealed trait FlushState {
  protected def order: Int
}
object FlushState {
  // There are new updates that need to be consumed before flushing or we just flushed
  object Idle extends FlushState { val order = 0 }
  // There were updates and we should invoke flush at some point if everything has been read
  object Updated extends FlushState { val order = 1 }
  // We were updated and we are trying to flush the system. If there is another update while the `AtHead` signal is
  // travelling through the graph, we will discard this and try again.
  object AtHead extends FlushState { val order = 2 }
  // Flush signal sent by Clock
  object ClockTick extends FlushState { val order = 3 }

  def max(left: FlushState, right: FlushState): FlushState =
    if (left.order > right.order) left else right

}

final case class BotUpdate[T](items: List[T], flush: FlushState)

object BaseDriver extends LazyLogging {

  abstract class Flusher[T] {
    def name: String
    def update(transaction: List[T]): Boolean
    def flush(): Boolean
    def isActive: Boolean
    def driver(): BotUpdate[T] => Boolean = {
      val level = new AtomicReference[FlushState](FlushState.Idle)
      upd => {
        // update logic: if there are new transactions, invoke the store update
        if (upd.items.nonEmpty) {
          val res = update(upd.items)
          // if the update function changed something, request a flush
          if (res) {
            level.set(FlushState.Updated)
          }
          res
        } else {
          // if there was nothing to update, then we are ready to flush. but pekko streams
          // buffers 17 messages by default between streams which means that the "batching"
          // will only start to merge transactions once it queued up 17 of them.
          // therefore, we do a small trick: we send a flush request again, but remember that
          // we didn't act on the previous one (FlushState.AtHead)
          // if in the meantime, we get an update, level will be reset to FlushState.Update
          // and the game starts again.
          // this guarantees that we only flush if we are at head
          if (
            (upd.flush == FlushState.Updated && level
              .get() == FlushState.Updated) || upd.flush == FlushState.ClockTick
          ) {
            level.set(FlushState.AtHead)
            true
          } else {
            if (level.get() == FlushState.AtHead && isActive) {
              val res = Try(flush()) match {
                case Success(ret) => ret
                case Failure(ex) =>
                  logger.error(s"Flush of $name failed with an exception", ex)
                  throw ex
              }
              level.set(FlushState.Idle)
              res
            } else
              false
          }
        }
      }
    }
  }

  def buildGraph[T](flush: BotUpdate[T] => Boolean): Flow[T, Unit, ?] = {

    val f = Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits.*

      // receive transactions and map them to right either
      val input = b.add(Flow[T].map(tx => Right(tx): Either[FlushState, T]))
      // merge our signals coming from the transaction stream, the clock tick and the flush process itself
      val merge = b.add(Merge[Either[FlushState, T]](3, eagerComplete = true).async)
      // batch transactions together if they start to pile up just before the flush process
      val batcher = Flow[Either[FlushState, T]]
        .batch(
          20,
          {
            case Left(flushState) => BotUpdate[T](List(), flushState)
            case Right(transaction) =>
              BotUpdate[T](List(transaction), FlushState.Idle)
          },
        )((agg, elem) =>
          elem match {
            case Left(flushState) => agg.copy(flush = FlushState.max(flushState, agg.flush))
            case Right(transaction) => agg.copy(items = agg.items :+ transaction)
          }
        )
      val batch = b.add(batcher.async)
      // process (update or flush)
      val process = b.add(Flow[BotUpdate[T]].map(flush).async)
      // filter out no-ops after flush
      val filter = b.add(Flow[Boolean].filter(x => x).map(_ => ()))
      // fan out so we can present an irrelevant sink
      val bcast = b.add(Broadcast[Unit](2))
      // map our flush request (created using a boolean) to FlushState.Updated
      val formulate = b.add(Flow[Unit].map(_ => Left(FlushState.Updated): Either[FlushState, T]))
      // tick source so that we flush for sure once every second
      val tick =
        Source.tick(1.seconds, 100.milliseconds, Left(FlushState.ClockTick): Either[FlushState, T])
      // i'm soo proud, my first pekko stream
      (input ~> merge ~> batch ~> process ~> filter ~> bcast ~> formulate ~> merge).discard
      // schedule regular ticks
      (tick ~> merge).discard

      FlowShape(input.in, bcast.out(1))
    })
    f
  }
}
