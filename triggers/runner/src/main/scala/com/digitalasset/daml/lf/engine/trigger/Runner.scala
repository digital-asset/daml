// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

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
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import com.daml.api.util.TimeProvider
import com.daml.lf.{CompiledPackages, PureCompiledPackages}
import com.daml.lf.archive.Dar
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref._
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.{Compiler, InitialSeeding, Pretty, SExpr, SValue, Speedy}
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.event._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.services.commands.CompletionStreamElement._
import com.daml.platform.participant.util.LfEngineToApi.toApiIdentifier
import com.daml.platform.services.time.TimeProviderType

import com.google.protobuf.empty.Empty

sealed trait TriggerMsg
final case class CompletionMsg(c: Completion) extends TriggerMsg
final case class TransactionMsg(t: Transaction) extends TriggerMsg
final case class HeartbeatMsg() extends TriggerMsg

final case class TypedExpr(expr: Expr, ty: TypeConApp)

final case class Trigger(
    expr: TypedExpr,
    triggerIds: TriggerIds,
    filters: Filters, // We store Filters rather than
    // TransactionFilter since the latter is
    // party-specific.
    heartbeat: Option[FiniteDuration]
)

// Utilities for interacting with the speedy machine.
object Machine extends StrictLogging {
  // Run speedy until we arrive at a value.
  def stepToValue(machine: Speedy.Machine): SValue = {
    machine.run() match {
      case SResultFinalValue(v) => v
      case SResultError(err) => {
        logger.error(Pretty.prettyError(err, machine.ptx).render(80))
        throw err
      }
      case res => {
        val errMsg = s"Unexpected speedy result: $res"
        logger.error(errMsg)
        throw new RuntimeException(errMsg)
      }
    }
  }
}

object Trigger extends StrictLogging {
  def fromIdentifier(
      compiledPackages: CompiledPackages,
      triggerId: Identifier): Either[String, Trigger] = {

    // Given an identifier to a high- or lowlevel trigger,
    // return an expression that will run the corresponding trigger
    // as a low-level trigger (by applying runTrigger) and the type of that expression.
    def detectTriggerType(tyCon: TypeConName, tyArg: Type): Either[String, TypedExpr] = {
      val triggerIds = TriggerIds(tyCon.packageId)
      if (tyCon == triggerIds.damlTriggerLowLevel("Trigger")) {
        logger.debug("Running low-level trigger")
        val expr = EVal(triggerId)
        val ty = TypeConApp(tyCon, ImmArray(tyArg))
        Right(TypedExpr(expr, ty))
      } else if (tyCon == triggerIds.damlTrigger("Trigger")) {
        logger.debug("Running high-level trigger")

        val runTrigger = EVal(triggerIds.damlTrigger("runTrigger"))
        val expr = EApp(runTrigger, EVal(triggerId))

        val triggerState = TTyCon(triggerIds.damlTriggerInternal("TriggerState"))
        val stateTy = TApp(triggerState, tyArg)
        val lowLevelTriggerTy = triggerIds.damlTriggerLowLevel("Trigger")
        val ty = TypeConApp(lowLevelTriggerTy, ImmArray(stateTy))

        Right(TypedExpr(expr, ty))
      } else {
        Left(s"Unexpected trigger type constructor $tyCon")
      }
    }

    val compiler = compiledPackages.compiler
    for {
      pkg <- compiledPackages
        .getPackage(triggerId.packageId)
        .toRight(s"Could not find package: ${triggerId.packageId}")
      definition <- pkg.lookupIdentifier(triggerId.qualifiedName)
      expr <- definition match {
        case DValue(TApp(TTyCon(tcon), stateTy), _, _, _) => detectTriggerType(tcon, stateTy)
        case DValue(ty, _, _, _) => Left(s"$ty is not a valid type for a trigger")
        case _ => Left(s"Trigger must points to a value but points to $definition")
      }
      triggerIds = TriggerIds(expr.ty.tycon.packageId)
      converter: Converter = Converter(compiledPackages, triggerIds)
      filter <- getTriggerFilter(compiledPackages, compiler, converter, expr)
      heartbeat <- getTriggerHeartbeat(compiledPackages, compiler, converter, expr)
    } yield Trigger(expr, triggerIds, filter, heartbeat)
  }

  // Return the heartbeat specified by the user.
  private def getTriggerHeartbeat(
      compiledPackages: CompiledPackages,
      compiler: Compiler,
      converter: Converter,
      expr: TypedExpr): Either[String, Option[FiniteDuration]] = {
    val heartbeat = compiler.unsafeCompile(
      ERecProj(expr.ty, Name.assertFromString("heartbeat"), expr.expr)
    )
    val machine = Speedy.Machine.fromSExpr(
      sexpr = heartbeat,
      compiledPackages = compiledPackages,
      submissionTime = Timestamp.now(),
      seeding = InitialSeeding.NoSeed,
      Set.empty,
    )
    Machine.stepToValue(machine) match {
      case SOptional(None) => Right(None)
      case SOptional(Some(relTime)) => converter.toFiniteDuration(relTime).map(Some(_))
      case value => Left(s"Expected Optional but got $value.")
    }
  }

  // Return the trigger filter specified by the user.
  def getTriggerFilter(
      compiledPackages: CompiledPackages,
      compiler: Compiler,
      converter: Converter,
      expr: TypedExpr): Either[String, Filters] = {
    val registeredTemplates =
      compiler.unsafeCompile(
        ERecProj(expr.ty, Name.assertFromString("registeredTemplates"), expr.expr))
    val machine =
      Speedy.Machine.fromSExpr(
        sexpr = registeredTemplates,
        compiledPackages = compiledPackages,
        submissionTime = Timestamp.now(),
        seeding = InitialSeeding.NoSeed,
        Set.empty,
      )
    Machine.stepToValue(machine) match {
      case SVariant(_, "AllInDar", _, _) => {
        val packages: Seq[(PackageId, Package)] = compiledPackages.packageIds
          .map(pkgId => (pkgId, compiledPackages.getPackage(pkgId).get))
          .toSeq
        val templateIds = packages.flatMap({
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
        Right(Filters(Some(InclusiveFilters(templateIds))))
      }
      case SVariant(_, "RegisteredTemplates", _, v) =>
        converter.toRegisteredTemplates(v) match {
          case Right(tpls) => Right(Filters(Some(InclusiveFilters(tpls.map(toApiIdentifier(_))))))
          case Left(err) => Left(err)
        }
      case v => Left(s"Expected AllInDar or RegisteredTemplates but got $v")
    }
  }
}

class Runner(
    compiledPackages: CompiledPackages,
    trigger: Trigger,
    client: LedgerClient,
    timeProviderType: TimeProviderType,
    applicationId: ApplicationId,
    party: String,
) extends StrictLogging {
  // Compiles LF expressions into Speedy expressions.
  private val compiler: Compiler = compiledPackages.compiler
  // Converts between various objects and SValues.
  private val converter: Converter = Converter(compiledPackages, trigger.triggerIds)
  // This is a map from the command IDs used on the ledger API to the
  // command IDs used internally in the trigger which are just
  // incremented at each step. This map can grow without bound. TODO :
  // limit the size.
  private var commandIdMap: Map[UUID, String] = Map.empty
  // This is the set of command IDs emitted by the trigger.  We track
  // this to detect collisions.
  private var usedCommandIds: Set[String] = Set.empty
  private var transactionFilter: TransactionFilter =
    TransactionFilter(Seq((party, trigger.filters)).toMap)

  // Handles the result of initialState or update, i.e., (s, [Commands], Text)
  // by submitting the commands, printing the log message and returning
  // the new state
  private def handleStepResult(v: SValue, submit: SubmitRequest => Unit): SValue =
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

  // This function produces a pair of a source of trigger messages
  // and a function to call in the event of a command submission
  // failure.
  private def msgSource(
      client: LedgerClient,
      offset: LedgerOffset,
      heartbeat: Option[FiniteDuration],
      party: String,
      filter: TransactionFilter)(implicit materializer: Materializer)
    : (Source[TriggerMsg, NotUsed], (String, StatusRuntimeException) => Unit) = {

    // A queue for command submission failures together with a source
    // from which messages posted to the queue can be consumed.
    val (completionQueue, completionQueueSource): (
        SourceQueue[Completion],
        Source[Completion, NotUsed]) =
      Source.queue[Completion](10, OverflowStrategy.backpressure).preMaterialize()
    // This function, given a command ID and a runtime exception,
    // posts to the completion queue.
    def postSubmitFailure(commandId: String, s: StatusRuntimeException) = {
      val _ = completionQueue.offer(
        Completion(
          commandId,
          Some(Status(s.getStatus().getCode().value(), s.getStatus().getDescription()))))
    }

    // The transaction source (ledger).
    val transactionSource: Source[TriggerMsg, NotUsed] =
      client.transactionClient
        .getTransactions(offset, None, filter, verbose = true)
        .map(TransactionMsg)

    // Command completion source (ledger completion stream +
    // synchronous sumbmission failures).
    val completionSource: Source[TriggerMsg, NotUsed] =
      client.commandClient
        .completionSource(List(party), offset)
        .mapConcat({
          case CheckpointElement(_) => List()
          case CompletionElement(c) => List(c)
        })
        .merge(completionQueueSource)
        .map(CompletionMsg)

    // Hearbeats source (we produce these repetitvely on a timer with
    // the given delay interval).
    val heartbeatSource: Source[TriggerMsg, NotUsed] = heartbeat match {
      case Some(interval) =>
        Source
          .tick[TriggerMsg](interval, interval, HeartbeatMsg())
          .mapMaterializedValue(_ => NotUsed)
      case None => Source.empty[TriggerMsg]
    }

    val triggerMsgSource = transactionSource.merge(completionSource).merge(heartbeatSource)

    (triggerMsgSource, postSubmitFailure)
  }

  // A sink for trigger messages representing a process for the
  // accumulated state changes resulting from application of the
  // messages given the starting state represented by the ACS
  // argument.
  private def getTriggerSink(
      name: String,
      acs: Seq[CreatedEvent],
      submit: SubmitRequest => Unit,
  ): Sink[TriggerMsg, Future[SExpr]] = {
    logger.info(s"Trigger ${name} is running as ${party}")

    // Compile the trigger initialState and Update LF functions to
    // speedy expressions.
    val update: SExpr =
      compiler.unsafeCompile(
        ERecProj(trigger.expr.ty, Name.assertFromString("update"), trigger.expr.expr))
    val getInitialState: SExpr =
      compiler.unsafeCompile(
        ERecProj(trigger.expr.ty, Name.assertFromString("initialState"), trigger.expr.expr))
    // Prepare a speedy machine for evaluting expressions.
    var machine: Speedy.Machine = Speedy.Machine.fromSExpr(
      sexpr = null,
      compiledPackages = compiledPackages,
      submissionTime = Timestamp.now(),
      seeding = InitialSeeding.NoSeed,
      Set.empty,
    )
    // Convert the ACS to a speedy expression.
    val createdExpr: SExpr = SEValue(converter.fromACS(acs) match {
      case Left(err) => throw new ConverterException(err)
      case Right(x) => x
    })
    val clientTime: Timestamp =
      Timestamp.assertFromInstant(Runner.getTimeProvider(timeProviderType).getCurrentTime)
    // Setup an application expression of initialState on the ACS.
    val initialState: SExpr =
      SEApp(
        getInitialState,
        Array(
          SEValue(SParty(Party.assertFromString(party))),
          SEValue(STimestamp(clientTime)): SExpr,
          createdExpr))
    // Evaluate it.
    machine.setExpressionToEvaluate(initialState)
    val value = Machine.stepToValue(machine)
    val evaluatedInitialState: SValue = handleStepResult(value, submit)
    logger.debug(s"Initial state: $evaluatedInitialState")

    // The flow that we return:
    //  - Maps incoming trigger messages to new trigger messages
    //    replacing ledger command IDs with the IDs used internally;
    //  - Folds over the trigger messages via the speedy machine
    //    thereby accumulating the state changes.
    // The materialized value of the flow is the (future) final state
    // of this process.
    Flow[TriggerMsg]
      .mapConcat[TriggerMsg]({
        case CompletionMsg(c) =>
          try {
            commandIdMap.get(UUID.fromString(c.commandId)) match {
              case None =>
                List()
              case Some(internalCommandId) =>
                List(CompletionMsg(c.copy(commandId = internalCommandId)))
            }
          } catch {
            // This happens for invalid UUIDs which we might get for
            // completions not emitted by the trigger.
            case e: IllegalArgumentException => List()
          }
        case TransactionMsg(t) =>
          try {
            commandIdMap.get(UUID.fromString(t.commandId)) match {
              case None =>
                List(TransactionMsg(t.copy(commandId = "")))
              case Some(internalCommandId) =>
                List(TransactionMsg(t.copy(commandId = internalCommandId)))
            }
          } catch {
            // This happens for invalid UUIDs which we might get for
            // transactions not emitted by the trigger.
            case e: IllegalArgumentException => List(TransactionMsg(t.copy(commandId = "")))
          }
        case x @ HeartbeatMsg() => List(x) // Hearbeats don't carry any information.
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
          case HeartbeatMsg() => converter.fromHeartbeat
        }
        val clientTime: Timestamp =
          Timestamp.assertFromInstant(Runner.getTimeProvider(timeProviderType).getCurrentTime)
        machine.setExpressionToEvaluate(
          SEApp(update, Array(SEValue(STimestamp(clientTime)): SExpr, SEValue(messageVal), state)))
        val value = Machine.stepToValue(machine)
        val newState = handleStepResult(value, submit)
        SEValue(newState)
      }))(Keep.right[NotUsed, Future[SExpr]])
  }

  // Query the ACS. This allows you to separate the initialization of
  // the initial state from the first run.
  def queryACS()(
      implicit materializer: Materializer,
      executionContext: ExecutionContext): Future[(Seq[CreatedEvent], LedgerOffset)] = {
    for {
      acsResponses <- client.activeContractSetClient
        .getActiveContracts(transactionFilter, verbose = true)
        .runWith(Sink.seq)
      offset = Array(acsResponses: _*).lastOption
        .fold(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))(resp =>
          LedgerOffset().withAbsolute(resp.offset))
    } yield (acsResponses.flatMap(x => x.activeContracts), offset)
  }

  // Run the trigger given the state of the ACS. The msgFlow argument
  // passed from ServiceMain is a kill switch. Other choices are
  // possible e.g. 'Flow[TriggerMsg].take()' and this fact is made use
  // of in the test-suite.
  def runWithACS[T](
      acs: Seq[CreatedEvent],
      offset: LedgerOffset,
      msgFlow: Graph[FlowShape[TriggerMsg, TriggerMsg], T] = Flow[TriggerMsg],
      name: String = "")(
      implicit materializer: Materializer,
      executionContext: ExecutionContext): (T, Future[SExpr]) = {
    val (source, postFailure) =
      msgSource(client, offset, trigger.heartbeat, party, transactionFilter)
    def submit(req: SubmitRequest): Unit = {
      val f: Future[Empty] = client.commandClient
        .withTimeProvider(Some(Runner.getTimeProvider(timeProviderType)))
        .submitSingleCommand(req)
      f.failed.foreach({
        case s: StatusRuntimeException =>
          postFailure(req.getCommands.commandId, s)
        case e =>
          logger.error(s"Unexpected exception: $e")
      })
    }
    source
      .viaMat(msgFlow)(Keep.right[NotUsed, T])
      .toMat(getTriggerSink(name, acs, submit))(Keep.both)
      .run()
  }
}

object Runner extends StrictLogging {
  // Return the time provider for a given time provider type.
  def getTimeProvider(ty: TimeProviderType): TimeProvider = {
    ty match {
      case TimeProviderType.Static => TimeProvider.Constant(Instant.EPOCH)
      case TimeProviderType.WallClock => TimeProvider.UTC
      case _ => throw new RuntimeException(s"Unexpected TimeProviderType: $ty")
    }
  }

  // Convience wrapper that creates the runner and runs the trigger.
  def run(
      dar: Dar[(PackageId, Package)],
      triggerId: Identifier,
      client: LedgerClient,
      timeProviderType: TimeProviderType,
      applicationId: ApplicationId,
      party: String
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[SExpr] = {
    val darMap = dar.all.toMap
    val compiledPackages = PureCompiledPackages(darMap).right.get
    val trigger = Trigger.fromIdentifier(compiledPackages, triggerId) match {
      case Left(err) => throw new RuntimeException(s"Invalid trigger: $err")
      case Right(trigger) => trigger
    }
    val runner =
      new Runner(compiledPackages, trigger, client, timeProviderType, applicationId, party)
    for {
      (acs, offset) <- runner.queryACS()
      finalState <- runner.runWithACS(acs, offset)._2
    } yield finalState
  }
}
