// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.trigger

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl._
import com.google.rpc.status.Status
import com.typesafe.scalalogging.StrictLogging
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.util.UUID
import scalaz.syntax.tag._
import scala.concurrent.{ExecutionContext, Future}

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.Compiler
import com.digitalasset.daml.lf.speedy.Pretty
import com.digitalasset.daml.lf.speedy.{SExpr, Speedy, SValue}
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId}
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.event._
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.services.commands.CompletionStreamElement._
import com.digitalasset.platform.services.time.TimeProviderType

sealed trait TriggerMsg
final case class CompletionMsg(c: Completion) extends TriggerMsg
final case class TransactionMsg(t: Transaction) extends TriggerMsg

class Runner(
    ledgerId: LedgerId,
    applicationId: ApplicationId,
    party: String,
    dar: Dar[(PackageId, Package)],
    submit: SubmitRequest => Unit)
    extends StrictLogging {

  private val converter = Converter.fromDar(dar)
  private val triggerIds = TriggerIds.fromDar(dar)
  if (triggerIds.triggerPackageId != EXPECTED_TRIGGER_PACKAGE_ID) {
    logger.warn(
      s"Unexpected package id for daml-trigger library: ${triggerIds.triggerPackageId}, expected ${EXPECTED_TRIGGER_PACKAGE_ID.toString}. This is most likely caused by a mismatch between the SDK version used to build your trigger and the trigger runner.")
  }
  private val darMap: Map[PackageId, Package] = dar.all.toMap
  private val compiler = Compiler(darMap)
  private val compiledPackages =
    PureCompiledPackages(darMap, compiler.compilePackages(darMap.keys)).right.get
  // This is a map from the command ids used on the ledger API to the command ids used internally
  // in the trigger which are just incremented at each step.
  private var commandIdMap: Map[UUID, String] = Map.empty
  // This is the set of command ids emitted by the trigger.
  // We track this to detect collisions.
  private var usedCommandIds: Set[String] = Set.empty

  // Handles the result of initialState or update, i.e., (s, [Commands], Text)
  // by submitting the commands, printing the log message and returning
  // the new state
  def handleStepResult(v: SValue): SValue =
    v match {
      case SRecord(recordId, _, values)
          if recordId.qualifiedName ==
            QualifiedName(
              DottedName.assertFromString("DA.Types"),
              DottedName.assertFromString("Tuple2")) => {
        val newState = values.get(0)
        val commandVal = values.get(1)
        logger.debug(s"New state: $newState")
        commandVal match {
          case SList(transactions) =>
            // Each transaction is a list of commands
            for (commands <- transactions) {
              converter.toCommands(commands) match {
                case Left(err) => throw new RuntimeException(err)
                case Right((commandId, commands)) => {
                  if (usedCommandIds.contains(commandId)) {
                    throw new RuntimeException(s"Duplicate command id: $commandId")
                  }
                  usedCommandIds += commandId
                  val commandUUID = UUID.randomUUID
                  commandIdMap += (commandUUID -> commandId)
                  val commandsArg = Commands(
                    ledgerId = ledgerId.unwrap,
                    applicationId = applicationId.unwrap,
                    commandId = commandUUID.toString,
                    party = party,
                    commands = commands
                  )
                  submit(SubmitRequest(commands = Some(commandsArg)))
                }
              }
            }
          case _ => {}
        }
        newState
      }
      case v => {
        throw new RuntimeException(s"Expected Tuple2 but got $v")
      }
    }

  def logTraces(machine: Speedy.Machine) = {
    machine.traceLog.iterator.foreach {
      case (msg, optLoc) =>
        logger.info(s"TRACE ${Pretty.prettyLoc(optLoc).render(80)}: $msg")
    }
  }

  def stepToValue(machine: Speedy.Machine) = {
    while (!machine.isFinal) {
      machine.step() match {
        case SResultContinue => ()
        case SResultError(err) => {
          logTraces(machine)
          logger.error(Pretty.prettyError(err, machine.ptx).render(80))
          throw err
        }
        case res => {
          logTraces(machine)
          val errMsg = s"Unexpected speedy result: $res"
          logger.error(errMsg)
          throw new RuntimeException(errMsg)
        }
      }
    }
    logTraces(machine)
  }

  def getTrigger(triggerId: Identifier): (Expr, TypeConApp) = {
    val (tyCon: TypeConName, stateTy) =
      dar.main._2.lookupIdentifier(triggerId.qualifiedName).toOption match {
        case Some(DValue(TApp(TTyCon(tcon), stateTy), _, _, _)) => (tcon, stateTy)
        case _ => {
          val errMsg = s"Identifier ${triggerId.qualifiedName} does not point to a trigger"
          throw new RuntimeException(errMsg)
        }
      }
    if (tyCon == triggerIds.getId("Trigger")) {
      logger.debug("Running low-level trigger")
      val triggerVal = EVal(triggerId)
      val triggerTy = TypeConApp(tyCon, ImmArray(stateTy))
      (triggerVal, triggerTy)
    } else if (tyCon == triggerIds.getHighlevelId("Trigger")) {
      logger.debug("Running high-level trigger")
      val lowTriggerVal = EApp(EVal(triggerIds.getHighlevelId("runTrigger")), EVal(triggerId))
      val lowStateTy = TApp(TTyCon(triggerIds.getHighlevelId("TriggerState")), stateTy)
      val lowTriggerTy = TypeConApp(triggerIds.getId("Trigger"), ImmArray(lowStateTy))
      (lowTriggerVal, lowTriggerTy)
    } else {
      val errMsg =
        s"Identifier ${triggerId.qualifiedName} does not point to a trigger. Its type must be Daml.Trigger.Trigger or Daml.Trigger.LowLevel.Trigger."
      throw new RuntimeException(errMsg)
    }
  }

  def getTriggerSink(
      triggerId: Identifier,
      acs: Seq[CreatedEvent]): Sink[TriggerMsg, Future[SExpr]] = {
    logger.info(s"Trigger is running as ${party}")
    val (triggerExpr, triggerTy) = getTrigger(triggerId)
    val update = compiler.compile(ERecProj(triggerTy, Name.assertFromString("update"), triggerExpr))
    val getInitialState =
      compiler.compile(ERecProj(triggerTy, Name.assertFromString("initialState"), triggerExpr))

    val machine = Speedy.Machine.fromSExpr(null, false, compiledPackages)
    val createdExpr: SExpr = SEValue(converter.fromACS(acs))
    val initialState =
      SEApp(getInitialState, Array(SEValue(SParty(Party.assertFromString(party))), createdExpr))
    machine.ctrl = Speedy.CtrlExpr(initialState)
    stepToValue(machine)
    val evaluatedInitialState = handleStepResult(machine.toSValue)
    logger.debug(s"Initial state: $evaluatedInitialState")
    Flow[TriggerMsg]
      .mapConcat[TriggerMsg]({
        case CompletionMsg(c) =>
          try {
            commandIdMap.get(UUID.fromString(c.commandId)) match {
              case None => List()
              case Some(internalCommandId) =>
                List(CompletionMsg(c.copy(commandId = internalCommandId)))
            }
          } catch {
            // This happens for invalid UUIDs which we might get for completions not emitted by the trigger.
            case e: IllegalArgumentException => List()
          }
        case msg @ TransactionMsg(_) => List(msg)
      })
      .toMat(Sink.fold[SExpr, TriggerMsg](SEValue(evaluatedInitialState))((state, message) => {
        val messageVal = message match {
          case TransactionMsg(transaction) => {
            converter.fromTransaction(transaction)
          }
          case CompletionMsg(completion) => {
            val status = completion.getStatus
            if (status.code != 0) {
              logger.warn(s"Command failed: ${status.message}, code: ${status.code}")
            }
            converter.fromCompletion(completion)
          }
        }
        machine.ctrl = Speedy.CtrlExpr(SEApp(update, Array(SEValue(messageVal), state)))
        stepToValue(machine)
        val newState = handleStepResult(machine.toSValue)
        SEValue(newState)
      }))(Keep.right[NotUsed, Future[SExpr]])
  }
}

object Runner extends StrictLogging {
  def getTimeProvider(ty: TimeProviderType): TimeProvider = {
    ty match {
      case TimeProviderType.Static => TimeProvider.Constant(Instant.EPOCH)
      case TimeProviderType.WallClock => TimeProvider.UTC
      case _ => throw new RuntimeException(s"Unexpected TimeProviderType: $ty")
    }
  }
  def queryACS(client: LedgerClient, party: String)(
      implicit materializer: Materializer,
      executionContext: ExecutionContext) = {
    val filter = TransactionFilter(List((party, Filters.defaultInstance)).toMap)
    for {
      acsResponses <- client.activeContractSetClient
        .getActiveContracts(filter, verbose = true)
        .runWith(Sink.seq)
      offset = Array(acsResponses: _*).lastOption
        .fold(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))(resp =>
          LedgerOffset().withAbsolute(resp.offset))
    } yield (acsResponses.flatMap(x => x.activeContracts), offset)
  }
  def run(
      dar: Dar[(PackageId, Package)],
      triggerId: Identifier,
      client: LedgerClient,
      timeProviderType: TimeProviderType,
      applicationId: ApplicationId,
      party: String,
      msgFlow: Flow[TriggerMsg, TriggerMsg, NotUsed] = Flow[TriggerMsg],
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[SExpr] = {
    val filter = TransactionFilter(List((party, Filters.defaultInstance)).toMap)
    for {
      (acs, offset) <- Runner.queryACS(client, party)
      finalState <- Runner.runWithACS(
        dar,
        triggerId,
        client,
        timeProviderType,
        applicationId,
        party,
        acs,
        offset)
    } yield finalState
  }
  def runWithACS(
      dar: Dar[(PackageId, Package)],
      triggerId: Identifier,
      client: LedgerClient,
      timeProviderType: TimeProviderType,
      applicationId: ApplicationId,
      party: String,
      acs: Seq[CreatedEvent],
      offset: LedgerOffset,
      msgFlow: Flow[TriggerMsg, TriggerMsg, NotUsed] = Flow[TriggerMsg],
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[SExpr] = {
    val filter = TransactionFilter(List((party, Filters.defaultInstance)).toMap)
    val (msgSource, postFailure) = Runner.msgSource(client, offset, party)
    val runner = new Runner(
      client.ledgerId,
      applicationId,
      party,
      dar,
      submitRequest => {
        val f = client.commandClient
          .withTimeProvider(Some(getTimeProvider(timeProviderType)))
          .submitSingleCommand(submitRequest)
        f.failed.foreach({
          case s: StatusRuntimeException =>
            postFailure(submitRequest.getCommands.commandId, s)
          case e => logger.error(s"Unexpected exception: $e")
        })
      }
    )
    msgSource
      .via(msgFlow)
      .runWith(runner.getTriggerSink(triggerId, acs))
  }
  def msgSource(client: LedgerClient, offset: LedgerOffset, party: String)(
      implicit materializer: Materializer)
    : (Source[TriggerMsg, NotUsed], (String, StatusRuntimeException) => Unit) = {
    // We use the queue to post failures that occur directly on command submission as opposed to
    // appearing asynchronously on the completion stream
    val (completionQueue, completionQueueSource) =
      Source.queue[Completion](10, OverflowStrategy.backpressure).preMaterialize()
    val transactionSource =
      client.transactionClient
        .getTransactions(
          offset,
          None,
          TransactionFilter(List((party, Filters.defaultInstance)).toMap),
          verbose = true)
        .map[TriggerMsg](TransactionMsg)
    val completionSource =
      client.commandClient
        .completionSource(List(party), offset)
        .mapConcat({
          case CheckpointElement(_) => List()
          case CompletionElement(c) => List(c)
        })
        .merge(completionQueueSource)
        .map[TriggerMsg](CompletionMsg)
    def postSubmitFailure(commandId: String, s: StatusRuntimeException) = {
      val _ = completionQueue.offer(
        Completion(
          commandId,
          Some(Status(s.getStatus().getCode().value(), s.getStatus().getDescription()))))
    }
    (transactionSource.merge(completionSource), postSubmitFailure)
  }
}
