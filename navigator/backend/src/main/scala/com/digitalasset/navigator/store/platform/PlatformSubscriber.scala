// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.store.platform

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash}
import akka.stream._
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import com.daml.lf.archive.Reader
import com.daml.lf.data.{Ref => DamlLfRef}
import com.daml.lf.iface.reader.{Errors, InterfaceReader}
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.package_service.GetPackageResponse
import com.daml.ledger.api.{v1 => V1}
import com.daml.ledger.client.LedgerClient
import com.daml.navigator.model._
import com.daml.navigator.model.converter.TypeNotFoundError
import com.daml.navigator.store.Store.{StoreException, _}
import com.daml.util.Ctx
import scalaz.Tag
import scalaz.syntax.tag._

import scala.concurrent.Future
import scala.util.{Failure, Success}

object PlatformSubscriber {
  // Actor messages
  case object ConnectionReset
  case class Started(commandTracker: TrackCommandsSource)
  case class SubmitCommand(command: Command, sender: ActorRef)

  // Actor state
  case class StateRunning(commandTracker: TrackCommandsSource)

  type TrackCommandsSource = SourceQueueWithComplete[Ctx[Command, SubmitRequest]]

  def props(
      ledgerClient: LedgerClient,
      party: PartyState,
      applicationId: DamlLfRef.LedgerString,
      token: Option[String]
  ) =
    Props(classOf[PlatformSubscriber], ledgerClient, party, applicationId, token)
}

/** Actor subscribing to platform event stream of a single DA party. */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class PlatformSubscriber(
    ledgerClient: LedgerClient,
    party: PartyState,
    applicationId: DamlLfRef.LedgerString,
    token: Option[String])
    extends Actor
    with ActorLogging
    with Stash {

  // ----------------------------------------------------------------------------------------------
  // Global immutable state - mutable state is stored in parameters of the receive methods
  // ----------------------------------------------------------------------------------------------
  private val system = context.system
  private val killSwitch = KillSwitches.shared("platform-subscriber")

  import system.dispatcher

  implicit val materializer: Materializer = Materializer(system)
  import PlatformSubscriber._

  // ----------------------------------------------------------------------------------------------
  // Lifecycle
  // ----------------------------------------------------------------------------------------------
  override def preStart(): Unit = {
    log.debug("Starting actor for '{}'", party.name)

    val started = for {
      _ <- fetchPackages(ledgerClient)
      tracker <- startTrackingCommands()
      _ <- startStreamingTransactions()
    } yield {
      Started(tracker)
    }

    started onComplete {
      case Success(value) =>
        log.debug("Started actor for '{}'", party.name)
        self ! value
      case Failure(error) =>
        // Failed to start up, giving up
        log.error(
          "Failed to start actor for party '{}': {}. Please fix any issues and restart this application.",
          party.name,
          error)
        context.become(failed(error))
    }
  }

  override def postStop(): Unit = {
    log.debug("Stopped actor for '{}'", party.name)
  }

  // ----------------------------------------------------------------------------------------------
  // Messages
  // ----------------------------------------------------------------------------------------------
  override def receive: Receive = initial

  def initial: Receive = {
    case Started(tracker) =>
      context.become(running(StateRunning(tracker)))
      unstashAll()

    case GetPartyActorInfo =>
      sender ! PartyActorStarting(party.name)

    case _ =>
      stash
  }

  def running(state: StateRunning): Receive = {
    case ResetConnection =>
      log.debug("Resetting connection for '{}'", party.name)
      killSwitch.shutdown()
      context.stop(self)
      sender ! ConnectionReset

    case SubmitCommand(command, commandSender) =>
      // Submit command and reply to
      submitCommand(ledgerClient, state.commandTracker, party, command, commandSender)

    case GetPartyActorInfo =>
      sender ! PartyActorStarted(party.name)
  }

  // Permanently failed state
  def failed(error: Throwable): Receive = {
    case GetApplicationStateInfo =>
      sender ! PartyActorFailed(party.name, error)

    case _ => ()
  }

  // ----------------------------------------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------------------------------------

  /** This has two side effects:
    * - each converted transaction is added to PartyState.ledger
    * - if packages need to be re-fetched, PartyState.packageRegistry is updated
    */
  private def processTransaction[Tx](
      id: String,
      transaction: Tx,
      reader: (Tx, converter.LedgerApiV1.Context) => Either[converter.ConversionError, Transaction]
  ): Future[NotUsed] = {
    def go(retryMissingTemplate: Boolean): Future[NotUsed] = {
      val ttx =
        reader(transaction, converter.LedgerApiV1.Context(party.name, party.packageRegistry))
      ttx match {
        case Right(tx) =>
          party.addLatestTransaction(tx)
          Future.successful(NotUsed)
        case Left(e: TypeNotFoundError) =>
          if (retryMissingTemplate) {
            log.info(
              "Template '{}' not found while processing transaction '{}', retrying after re-fetching packages.",
              e.id,
              id)
            fetchPackages(ledgerClient)
              .flatMap(_ => go(false))
          } else {
            Future.failed(e)
          }
        case Left(e) =>
          Future.failed(e)
      }
    }

    try {
      go(true).recoverWith {
        case e: Throwable =>
          log.error(
            "Error processing transaction {}: {}. Its effects will not be visible.",
            e.getMessage,
            id)
          Future.failed(e)
      }
    } catch {
      case e: Throwable =>
        log.error(
          "Error processing transaction {}: {}. Its effects will not be visible.",
          e.getMessage,
          id)
        Future.failed(e)
    }
  }

  private def startStreamingTransactions(): Future[Unit] = {
    val ledgerBegin = V1.ledger_offset.LedgerOffset(
      V1.ledger_offset.LedgerOffset.Value
        .Boundary(V1.ledger_offset.LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
    val transactionFilter = V1.transaction_filter.TransactionFilter(
      Map(Tag.unwrap(party.name) -> V1.transaction_filter.Filters(None))
    )

    // Create a source (transactions stream from ledger)
    val treeSource: Source[NotUsed, NotUsed] =
      ledgerClient.transactionClient
        .getTransactionTrees(ledgerBegin, None, transactionFilter, token = token)
        .mapAsync(1)(tx =>
          processTransaction(tx.transactionId, tx, converter.LedgerApiV1.readTransactionTree))

    // Run the stream.
    // The stream runs independently of the actor. Add a kill switch to terminate the stream when actor stops.
    // Note: processTransaction changes the PartyState as a side effect. The stream output can be ignored.
    treeSource
      .via(killSwitch.flow)
      .runWith(Sink.ignore)

    // This stream starts immediately
    Future.successful(())
  }

  private def startTrackingCommands()
    : Future[SourceQueueWithComplete[Ctx[Command, SubmitRequest]]] = {
    for {
      commandTracker <- ledgerClient.commandClient
        .trackCommands[Command](List(Tag.unwrap(party.name)), token)
    } yield
    // Note: this uses a buffer of 1000 commands awaiting handling by the command client.
    // The command client itself can process (config.maxParallelSubmissions) commands in parallel.
    // In the highly unlikely case both buffers are full, the command submission will fail immediately.
    Source
      .queue[Ctx[Command, SubmitRequest]](1000, OverflowStrategy.dropNew)
      .via(commandTracker)
      .map(result => {
        val completion = converter.LedgerApiV1.readCompletion(result.value)
        val commandId = result.context.id
        completion match {
          case Right(Some(status)) =>
            // Command completed with an error
            party.addCommandStatus(commandId, status)
            log.info("Command '{}' completed with status '{}'.", commandId, status)
          case Right(None) =>
            // Command completed with success.
            // Don't update anything, state will be updated from the transaction stream instead.
            // This is because the completion stream does not contain information to correlate command id with transaction id.
            log.info("Command '{}' completed successfully.", commandId)
          case Left(error) =>
            party.addCommandStatus(commandId, CommandStatusUnknown())
            log.error(
              "Command tracking failed. Status unknown for command '{}': {}.",
              commandId,
              error)
        }
      })
      .to(Sink.ignore)
      .run()
  }

  /*
    Note about packages:
    - There may be packages uploaded at a later time
    - All parties see the same list of packages
    - The packages associated to package ids will never change
    - The package set should probably only grow, but it's not codified anywhere

    Whenever the client sees a template that it does not recognize in the transaction stream,
    it can re-fetch the packages from the server to get the metadata it needs to make sense of them.
   */
  private def fetchPackages(ledgerClient: LedgerClient): Future[Unit] = {
    ledgerClient.packageClient
      .listPackages(token)
      .flatMap(response => {
        Future.traverse(response.packageIds)(id => {
          party.packageRegistry.pack(DamlLfRef.PackageId.assertFromString(id)) match {
            case Some(_) =>
              Future.successful(None)
            case None =>
              ledgerClient.packageClient.getPackage(id, token).map(Some(_))
          }
        })
      })
      .flatMap(responses0 => {
        val interfaces = responses0
          .collect { case Some(resp) => resp }
          .map(decodePackage)
          .toList
        party.addPackages(interfaces)
        log.info(
          "Successfully loaded packages {}",
          interfaces.map(_.packageId).mkString("[", ", ", "]"))
        Future.successful(())
      })
      .recoverWith(apiFailureF)
  }

  private def decodePackage(res: GetPackageResponse) = {
    val cos = Reader.damlLfCodedInputStream(res.archivePayload.newInput)
    val payload = DamlLf.ArchivePayload.parseFrom(cos)
    val (errors, out) =
      InterfaceReader.readInterface(DamlLfRef.PackageId.assertFromString(res.hash) -> payload)
    if (!errors.equals(Errors.zeroErrors)) {
      log.error("Errors loading package {}: {}", res.hash, errors.toString)
    }
    out
  }

  private def submitCommand(
      ledgerClient: LedgerClient,
      commandTracker: TrackCommandsSource,
      party: PartyState,
      command: Command,
      sender: ActorRef
  ): Unit = {
    // Convert to ledger API command
    converter.LedgerApiV1
      .writeCommands(party, command, ledgerClient.ledgerId.unwrap, applicationId)
      .fold[Unit](
        error => {
          // Failed to convert command. Most likely, the argument is incomplete.
          sender ! Failure(error)
        },
        commands => {
          import akka.stream.{QueueOfferResult => QOR}

          // Store the command
          party.addCommand(command)

          // Send command to ledger
          commandTracker
            .offer(Ctx(command, V1.command_submission_service.SubmitRequest(Some(commands), None)))
            .andThen {
              case Success(QOR.Dropped) =>
                party.addCommandStatus(
                  command.id,
                  CommandStatusError("INTERNAL", "Command submission failed: buffer full"))
              case Success(QOR.QueueClosed) =>
                party.addCommandStatus(
                  command.id,
                  CommandStatusError("INTERNAL", "Command submission failed: queue closed"))
              case Success(QOR.Failure(e)) =>
                party.addCommandStatus(
                  command.id,
                  CommandStatusError("INTERNAL", s"Command submission failed: $e"))
              case Failure(e) =>
                party.addCommandStatus(
                  command.id,
                  CommandStatusError("INTERNAL", s"Command submission failed: $e"))
            }

          // Immediately return the command ID
          sender ! Success(command.id)
        }
      )
  }

  private def apiFailureF[T]: PartialFunction[Throwable, Future[T]] = {
    case exception: Exception =>
      log.error("Unable to perform API operation: {}", exception.getMessage)
      Future.failed(StoreException(exception.getMessage))
  }
}
