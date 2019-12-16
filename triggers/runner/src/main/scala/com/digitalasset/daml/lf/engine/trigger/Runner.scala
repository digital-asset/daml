// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.trigger

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
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.Compiler
import com.digitalasset.daml.lf.speedy.Pretty
import com.digitalasset.daml.lf.speedy.{SExpr, Speedy, SValue, TraceLog}
import com.digitalasset.daml.lf.speedy.SExpr._
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.ledger.api.refinements.ApiTypes.{ApplicationId}
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.event._
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.services.commands.CompletionStreamElement._
import com.digitalasset.platform.participant.util.LfEngineToApi.toApiIdentifier
import com.digitalasset.platform.services.time.TimeProviderType

sealed trait TriggerMsg
final case class CompletionMsg(c: Completion) extends TriggerMsg
final case class TransactionMsg(t: Transaction) extends TriggerMsg

class Runner(
    client: LedgerClient,
    applicationId: ApplicationId,
    party: String,
    dar: Dar[(PackageId, Package)],
) extends StrictLogging {

  private val triggerIds = TriggerIds.fromDar(dar)
  if (triggerIds.triggerPackageId != EXPECTED_TRIGGER_PACKAGE_ID) {
    logger.warn(
      s"Unexpected package id for daml-trigger library: ${triggerIds.triggerPackageId}, expected ${EXPECTED_TRIGGER_PACKAGE_ID.toString}. This is most likely caused by a mismatch between the SDK version used to build your trigger and the trigger runner.")
  }
  private val darMap: Map[PackageId, Package] = dar.all.toMap
  private val compiler = Compiler(darMap)
  private val compiledPackages =
    PureCompiledPackages(darMap, compiler.compilePackages(darMap.keys)).right.get
  private val converter = Converter.fromDar(dar, compiledPackages)
  // This is a map from the command ids used on the ledger API to the command ids used internally
  // in the trigger which are just incremented at each step.
  private var commandIdMap: Map[UUID, String] = Map.empty
  // This is the set of command ids emitted by the trigger.
  // We track this to detect collisions.
  private var usedCommandIds: Set[String] = Set.empty

  // Handles the result of initialState or update, i.e., (s, [Commands], Text)
  // by submitting the commands, printing the log message and returning
  // the new state
  def handleStepResult(v: SValue, submit: SubmitRequest => Unit): SValue =
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
                case Left(err) => throw new ConverterException(err)
                case Right((commandId, commands)) => {
                  if (usedCommandIds.contains(commandId)) {
                    throw new RuntimeException(s"Duplicate command id: $commandId")
                  }
                  usedCommandIds += commandId
                  val commandUUID = UUID.randomUUID
                  commandIdMap += (commandUUID -> commandId)
                  val commandsArg = Commands(
                    ledgerId = client.ledgerId.unwrap,
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

  def logTraces(machine: Speedy.Machine): Speedy.Machine = {
    var traceEmpty = true
    machine.traceLog.iterator.foreach {
      case (msg, optLoc) =>
        traceEmpty = false
        logger.info(s"TRACE ${Pretty.prettyLoc(optLoc).render(80)}: $msg")
    }
    // Right now there isn’t a way to reset the TraceLog so we have to manually replace it by a new empty tracelog.
    // We might want to make this possible in the future since it would be a bit cheaper but
    // this shouldn’t be the bottleneck in triggers.
    if (traceEmpty) {
      // We can avoid allocating a new TraceLog in the common case where there was no trace statement.
      machine
    } else {
      machine.copy(traceLog = TraceLog(machine.traceLog.capacity))
    }
  }

  def stepToValue(machine: Speedy.Machine): Speedy.Machine = {
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

  def getTriggerFilter(triggerId: Identifier): TransactionFilter = {
    val (triggerExpr, triggerTy) = getTrigger(triggerId)
    val registeredTemplates = compiler.compile(
      ERecProj(triggerTy, Name.assertFromString("registeredTemplates"), triggerExpr))
    var machine = Speedy.Machine.fromSExpr(registeredTemplates, false, compiledPackages)
    machine = stepToValue(machine)
    val templateIds = machine.toSValue match {
      case SVariant(_, "AllInDar", _) => {
        darMap.toList.flatMap({
          case (pkgId, pkg) =>
            pkg.modules.toList.flatMap({
              case (modName, module) =>
                module.definitions.toList.flatMap({
                  case (entityName, definition) =>
                    definition match {
                      case DDataType(_, _, DataRecord(_, Some(tpl))) =>
                        Seq(toApiIdentifier(Identifier(pkgId, QualifiedName(modName, entityName))))
                      case _ => Seq()
                    }
                })
            })
        })
      }
      case SVariant(_, "RegisteredTemplates", v) =>
        converter.toRegisteredTemplates(v) match {
          case Right(tpls) => tpls.map(toApiIdentifier(_))
          case Left(err) => throw new ConverterException(err)
        }
      case v => throw new ConverterException(s"Expected AllInDar or RegisteredTemplates but got $v")
    }
    TransactionFilter(List((party, Filters(Some(InclusiveFilters(templateIds))))).toMap)
  }

  def msgSource(
      client: LedgerClient,
      offset: LedgerOffset,
      party: String,
      filter: TransactionFilter)(implicit materializer: Materializer)
    : (Source[TriggerMsg, NotUsed], (String, StatusRuntimeException) => Unit) = {
    // We use the queue to post failures that occur directly on command submission as opposed to
    // appearing asynchronously on the completion stream
    val (completionQueue, completionQueueSource) =
      Source.queue[Completion](10, OverflowStrategy.backpressure).preMaterialize()
    val transactionSource =
      client.transactionClient
        .getTransactions(offset, None, filter, verbose = true)
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

  def getTriggerSink(
      triggerId: Identifier,
      timeProviderType: TimeProviderType,
      acs: Seq[CreatedEvent],
      submit: SubmitRequest => Unit,
  ): Sink[TriggerMsg, Future[SExpr]] = {
    logger.info(s"Trigger is running as ${party}")
    val (triggerExpr, triggerTy) = getTrigger(triggerId)
    val update = compiler.compile(ERecProj(triggerTy, Name.assertFromString("update"), triggerExpr))
    val getInitialState =
      compiler.compile(ERecProj(triggerTy, Name.assertFromString("initialState"), triggerExpr))

    var machine = Speedy.Machine.fromSExpr(null, false, compiledPackages)
    val createdExpr: SExpr = SEValue(converter.fromACS(acs) match {
      case Left(err) => throw new ConverterException(err)
      case Right(x) => x
    })
    val clientTime: Timestamp =
      Timestamp.assertFromInstant(Runner.getTimeProvider(timeProviderType).getCurrentTime)
    val initialState =
      SEApp(
        getInitialState,
        Array(
          SEValue(SParty(Party.assertFromString(party))),
          SEValue(STimestamp(clientTime)): SExpr,
          createdExpr))
    machine.ctrl = Speedy.CtrlExpr(initialState)
    machine = stepToValue(machine)
    val evaluatedInitialState = handleStepResult(machine.toSValue, submit)
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
        case TransactionMsg(t) =>
          try {
            commandIdMap.get(UUID.fromString(t.commandId)) match {
              case None => List(TransactionMsg(t.copy(commandId = "")))
              case Some(internalCommandId) =>
                List(TransactionMsg(t.copy(commandId = internalCommandId)))
            }
          } catch {
            // This happens for invalid UUIDs which we might get for transactions not emitted by the trigger.
            case e: IllegalArgumentException => List(TransactionMsg(t.copy(commandId = "")))
          }
      })
      .toMat(Sink.fold[SExpr, TriggerMsg](SEValue(evaluatedInitialState))((state, message) => {
        val messageVal = message match {
          case TransactionMsg(transaction) => {
            converter.fromTransaction(transaction) match {
              case Left(err) => throw new ConverterException(err)
              case Right(x) => x
            }
          }
          case CompletionMsg(completion) => {
            val status = completion.getStatus
            if (status.code != 0) {
              logger.warn(s"Command failed: ${status.message}, code: ${status.code}")
            }
            converter.fromCompletion(completion) match {
              case Left(err) => throw new ConverterException(err)
              case Right(x) => x
            }
          }
        }
        val clientTime: Timestamp =
          Timestamp.assertFromInstant(Runner.getTimeProvider(timeProviderType).getCurrentTime)
        machine.ctrl = Speedy.CtrlExpr(
          SEApp(update, Array(SEValue(STimestamp(clientTime)): SExpr, SEValue(messageVal), state)))
        machine = stepToValue(machine)
        val newState = handleStepResult(machine.toSValue, submit)
        SEValue(newState)
      }))(Keep.right[NotUsed, Future[SExpr]])
  }

  def queryACS(client: LedgerClient, filter: TransactionFilter)(
      implicit materializer: Materializer,
      executionContext: ExecutionContext) = {
    for {
      acsResponses <- client.activeContractSetClient
        .getActiveContracts(filter, verbose = true)
        .runWith(Sink.seq)
      offset = Array(acsResponses: _*).lastOption
        .fold(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))(resp =>
          LedgerOffset().withAbsolute(resp.offset))
    } yield (acsResponses.flatMap(x => x.activeContracts), offset)
  }

  def runWithACS(
      triggerId: Identifier,
      timeProviderType: TimeProviderType,
      acs: Seq[CreatedEvent],
      offset: LedgerOffset,
      filter: TransactionFilter,
      msgFlow: Flow[TriggerMsg, TriggerMsg, NotUsed] = Flow[TriggerMsg],
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[SExpr] = {
    val (source, postFailure) = msgSource(client, offset, party, filter)
    def submit(req: SubmitRequest) = {
      val f = client.commandClient
        .withTimeProvider(Some(Runner.getTimeProvider(timeProviderType)))
        .submitSingleCommand(req)
      f.failed.foreach({
        case s: StatusRuntimeException =>
          postFailure(req.getCommands.commandId, s)
        case e => logger.error(s"Unexpected exception: $e")
      })
    }
    source
      .via(msgFlow)
      .runWith(getTriggerSink(triggerId, timeProviderType, acs, submit))
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
  def run(
      dar: Dar[(PackageId, Package)],
      triggerId: Identifier,
      client: LedgerClient,
      timeProviderType: TimeProviderType,
      applicationId: ApplicationId,
      party: String,
      msgFlow: Flow[TriggerMsg, TriggerMsg, NotUsed] = Flow[TriggerMsg],
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[SExpr] = {
    val runner = new Runner(
      client,
      applicationId,
      party,
      dar
    )
    val filter = runner.getTriggerFilter(triggerId)
    for {
      (acs, offset) <- runner.queryACS(client, filter)
      finalState <- runner.runWithACS(triggerId, timeProviderType, acs, offset, filter)
    } yield finalState
  }
}
