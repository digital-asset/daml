// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

import scalaz.{-\/, Functor, \/, \/-}
import scalaz.\/.fromTryCatchThrowable
import scalaz.std.option._
import scalaz.syntax.bifunctor._
import scalaz.syntax.functor._
import scalaz.syntax.tag._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

import scala.language.higherKinds

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import com.daml.api.util.TimeProvider
import com.daml.lf.{CompiledPackages, PureCompiledPackages}
import com.daml.lf.archive.Dar
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref._
import com.daml.lf.data.ScalazEqual._
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.{Compiler, Pretty, SExpr, SValue, Speedy}
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.v1.commands.{Command, Commands}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.event._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.services.commands.CompletionStreamElement._
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import LoggingContextOf.{label, newLoggingContext}
import com.daml.platform.participant.util.LfEngineToApi.toApiIdentifier
import com.daml.platform.services.time.TimeProviderType
import com.daml.script.converter.Converter.{DamlTuple2, DamlAnyModuleRecord, unrollFree}
import com.daml.script.converter.Converter.Implicits._
import com.daml.script.converter.ConverterException

import com.google.protobuf.empty.Empty

sealed trait TriggerMsg
final case class CompletionMsg(c: Completion) extends TriggerMsg
final case class TransactionMsg(t: Transaction) extends TriggerMsg
final case class HeartbeatMsg() extends TriggerMsg

final case class TypedExpr(expr: Expr, ty: TypeConApp)

final case class Trigger(
    expr: TypedExpr,
    triggerDefinition: Identifier,
    triggerIds: TriggerIds,
    filters: Filters, // We store Filters rather than
    // TransactionFilter since the latter is
    // party-specific.
    heartbeat: Option[FiniteDuration]
) {
  private[trigger] def loggingExtension: Map[String, String] =
    Map("triggerDefinition" -> triggerDefinition.toString)
}

// Utilities for interacting with the speedy machine.
object Machine extends StrictLogging {
  // Run speedy until we arrive at a value.
  def stepToValue(machine: Speedy.Machine): SValue = {
    machine.run() match {
      case SResultFinalValue(v) => v
      case SResultError(err) => {
        logger.error(Pretty.prettyError(err).render(80))
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
        .getSignature(triggerId.packageId)
        .toRight(s"Could not find package: ${triggerId.packageId}")
      definition <- pkg.lookupDefinition(triggerId.qualifiedName)
      expr <- definition match {
        case DValueSignature(TApp(TTyCon(tcon), stateTy), _, _, _) =>
          detectTriggerType(tcon, stateTy)
        case DValueSignature(ty, _, _, _) => Left(s"$ty is not a valid type for a trigger")
        case _ => Left(s"Trigger must points to a value but points to $definition")
      }
      triggerIds = TriggerIds(expr.ty.tycon.packageId)
      converter: Converter = Converter(compiledPackages, triggerIds)
      filter <- getTriggerFilter(compiledPackages, compiler, converter, expr)
      heartbeat <- getTriggerHeartbeat(compiledPackages, compiler, converter, expr)
    } yield Trigger(expr, triggerId, triggerIds, filter, heartbeat)
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
    val machine = Speedy.Machine.fromPureSExpr(compiledPackages, heartbeat)
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
    val machine = Speedy.Machine.fromPureSExpr(compiledPackages, registeredTemplates)
    Machine.stepToValue(machine) match {
      case SVariant(_, "AllInDar", _, _) => {
        val packages: Seq[(PackageId, PackageSignature)] = compiledPackages.packageIds
          .map(pkgId => (pkgId, compiledPackages.getSignature(pkgId).get))
          .toSeq
        val templateIds = packages.flatMap({
          case (pkgId, pkg) =>
            pkg.modules.toList.flatMap({
              case (modName, module) =>
                module.templates.keys.map(entityName =>
                  toApiIdentifier(Identifier(pkgId, QualifiedName(modName, entityName))))
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
)(implicit loggingContext: LoggingContextOf[Trigger]) {
  import Runner.{SeenMsgs, alterF}

  // Compiles LF expressions into Speedy expressions.
  private val compiler: Compiler = compiledPackages.compiler
  // Converts between various objects and SValues.
  private val converter: Converter = Converter(compiledPackages, trigger.triggerIds)
  // These are the command IDs used on the ledger API to submit commands for
  // this trigger for which we are awaiting either a completion or transaction
  // message, or both.
  private[this] var pendingCommandIds: Map[UUID, SeenMsgs] = Map.empty
  private val transactionFilter: TransactionFilter =
    TransactionFilter(Seq((party, trigger.filters)).toMap)

  private[this] def logger = ContextualizedLogger get getClass

  // return whether uuid *was* present in pendingCommandIds
  private[this] def useCommandId(uuid: UUID, seeOne: SeenMsgs.One): Boolean = {
    val newMap = alterF(pendingCommandIds, uuid)(_ map (_ see seeOne))
    newMap foreach { pendingCommandIds = _ }
    newMap.isDefined
  }

  import Runner.{
    DamlFun,
    SingleCommandFailure,
    maxParallelSubmissionsPerTrigger,
    maxTriesWhenOverloaded,
    overloadedRetryDelay,
    retrying,
  }

  @throws[RuntimeException]
  private def handleCommands(commands: Seq[Command]): (UUID, SubmitRequest) = {
    val commandUUID = UUID.randomUUID
    pendingCommandIds += ((commandUUID, SeenMsgs.Neither))
    val commandsArg = Commands(
      ledgerId = client.ledgerId.unwrap,
      applicationId = applicationId.unwrap,
      commandId = commandUUID.toString,
      party = party,
      commands = commands
    )
    logger.debug(
      s"submitting command ID ${commandUUID: UUID}, commands ${commands.map(_.command.value)}")
    (commandUUID, SubmitRequest(commands = Some(commandsArg)))
  }

  private def freeTriggerSubmits(
      clientTime: Timestamp,
      v: SValue): UnfoldState[SValue, SubmitRequest] = {
    def evaluate(se: SExpr) = {
      val machine: Speedy.Machine =
        Speedy.Machine.fromPureSExpr(compiledPackages, se)
      // Evaluate it.
      machine.setExpressionToEvaluate(se)
      Machine.stepToValue(machine)
    }
    @tailrec def go(v: SValue): SValue \/ (SubmitRequest, SValue) = {
      val resumed = unrollFree(v) match {
        case Right(Right(vvv @ (variant, vv))) =>
          vvv.match2 {
            case "GetTime" /*(Time -> a)*/ => {
              case DamlFun(timeA) => Right(evaluate(makeAppD(timeA, STimestamp(clientTime))))
            }
            case "Submit" /*([Command], Text -> a)*/ => {
              case DamlTuple2(sCommands, DamlFun(textA)) =>
                val commands = converter.toCommands(sCommands).orConverterException
                val (commandUUID, submitRequest) = handleCommands(commands)
                Left(\/-(
                  (submitRequest, evaluate(makeAppD(textA, SText((commandUUID: UUID).toString))))))
            }
            case _ =>
              val msg = s"unrecognized TriggerF step $variant"
              logger.error(msg)
              throw new ConverterException(msg)
          }(fallback = throw new ConverterException(s"invalid contents for $variant: $vv"))
        case Right(Left(newState)) => Left(-\/(newState))
        case Left(e) => throw new ConverterException(e)
      }
      resumed match {
        case Left(newState) => newState
        case Right(suspended) => go(suspended)
      }
    }

    UnfoldState(v)(go)
  }

  // This function produces a source of trigger messages,
  // with input of failures from command submission
  private def msgSource(
      client: LedgerClient,
      offset: LedgerOffset,
      heartbeat: Option[FiniteDuration],
      party: String,
      filter: TransactionFilter): Flow[SingleCommandFailure, TriggerMsg, NotUsed] = {

    // A queue for command submission failures.
    val submissionFailureQueue: Flow[SingleCommandFailure, Completion, NotUsed] =
      Flow[SingleCommandFailure]
      // 256 comes from the default ExecutionContext.
      // Why `fail`?  Consider the most obvious alternatives.
      //
      // `backpressure`?  This feeds into the Free interpreter flow, which may produce
      //   many command failures for one event, hence deadlock.
      //
      // `drop*`?  A trigger will proceed as if everything was fine, without ever
      //   getting the notification that its reaction to one contract and attempt to
      //   advance its workflow failed.
      //
      // `fail`, on the other hand?  It far better fits the trigger model to go
      //   tabula rasa and notice the still-unhandled contract in the ACS again
      //   on init, as we expect triggers to be able to do anyhow.
        .buffer(256 + maxParallelSubmissionsPerTrigger, OverflowStrategy.fail)
        .map {
          case SingleCommandFailure(commandId, s) =>
            Completion(
              commandId,
              Some(Status(s.getStatus().getCode().value(), s.getStatus().getDescription())))
        }

    // The transaction source (ledger).
    val transactionSource: Source[TriggerMsg, NotUsed] =
      client.transactionClient
        .getTransactions(offset, None, filter, verbose = false)
        .map(TransactionMsg)

    // Command completion source (ledger completion stream +
    // synchronous sumbmission failures).
    val completionSource: Flow[SingleCommandFailure, TriggerMsg, NotUsed] =
      submissionFailureQueue
        .merge(
          client.commandClient
            .completionSource(List(party), offset)
            .mapConcat {
              case CheckpointElement(_) => List()
              case CompletionElement(c) => List(c)
            })
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

    completionSource merge transactionSource merge heartbeatSource
  }

  private def logReceivedMsg(tm: TriggerMsg): Unit = tm match {
    case CompletionMsg(c) => logger.debug(s"trigger received completion message $c")
    case TransactionMsg(t) => logger.debug(s"trigger received transaction, ID ${t.transactionId}")
    case HeartbeatMsg() => ()
  }

  private[this] def getInitialStateFreeAndUpdate(acs: Seq[CreatedEvent]): (SValue, SValue) = {
    // Compile the trigger initialState and Update LF functions to
    // speedy expressions.
    val update: SExpr =
      compiler.unsafeCompile(
        ERecProj(trigger.expr.ty, Name.assertFromString("update"), trigger.expr.expr))
    val getInitialState: SExpr =
      compiler.unsafeCompile(
        ERecProj(trigger.expr.ty, Name.assertFromString("initialState"), trigger.expr.expr))
    // Convert the ACS to a speedy value.
    val createdValue: SValue = converter.fromACS(acs).orConverterException
    // Setup an application expression of initialState on the ACS.
    val initialState: SExpr =
      makeApp(getInitialState, Array(SParty(Party.assertFromString(party)), createdValue))
    // Prepare a speedy machine for evaluating expressions.
    val machine: Speedy.Machine =
      Speedy.Machine.fromPureSExpr(compiledPackages, initialState)
    // Evaluate it.
    machine.setExpressionToEvaluate(initialState)
    val initialStateFree = Machine
      .stepToValue(machine)
      .expect("TriggerSetup", {
        case DamlAnyModuleRecord("TriggerSetup", fts) => fts
      })
      .orConverterException
    machine.setExpressionToEvaluate(update)
    val evaluatedUpdate: SValue = Machine.stepToValue(machine)
    (initialStateFree, evaluatedUpdate)
  }

  private[this] def encodeMsgs: Flow[TriggerMsg, SValue, NotUsed] =
    Flow fromFunction {
      case TransactionMsg(transaction) =>
        converter.fromTransaction(transaction).orConverterException
      case CompletionMsg(completion) =>
        val status = completion.getStatus
        if (status.code != 0) {
          logger.warn(s"Command failed: ${status.message}, code: ${status.code}")
        }
        converter.fromCompletion(completion).orConverterException
      case HeartbeatMsg() => converter.fromHeartbeat
    }

  // A flow for trigger messages representing a process for the
  // accumulated state changes resulting from application of the
  // messages given the starting state represented by the ACS
  // argument.
  private def getTriggerEvaluator(
      name: String,
      acs: Seq[CreatedEvent],
  ): Flow[TriggerMsg, SubmitRequest, Future[SValue]] = {
    logger.info(s"Trigger ${name} is running as ${party}")

    val clientTime: Timestamp =
      Timestamp.assertFromInstant(Runner.getTimeProvider(timeProviderType).getCurrentTime)
    val (initialStateFree, evaluatedUpdate) = getInitialStateFreeAndUpdate(acs)

    // Prepare another speedy machine for evaluating expressions.
    val machine: Speedy.Machine =
      Speedy.Machine.fromPureSExpr(compiledPackages, SEValue(SUnit))

    import UnfoldState.{toSourceOps, toSource, flatMapConcatNodeOps, flatMapConcatNode}

    val runInitialState = toSource(freeTriggerSubmits(clientTime, initialStateFree))

    val runRuleOnMsgs = flatMapConcatNode { (state: SValue, messageVal: SValue) =>
      val clientTime: Timestamp =
        Timestamp.assertFromInstant(Runner.getTimeProvider(timeProviderType).getCurrentTime)
      machine.setExpressionToEvaluate(makeAppD(evaluatedUpdate, messageVal))
      val stateFun = Machine
        .stepToValue(machine)
        .expect("TriggerRule", {
          case DamlAnyModuleRecord("TriggerRule", DamlAnyModuleRecord("StateT", fun)) => fun
        })
        .orConverterException
      machine.setExpressionToEvaluate(makeAppD(stateFun, state))
      val updateWithNewState = Machine.stepToValue(machine)

      freeTriggerSubmits(clientTime, v = updateWithNewState)
        .leftMap(_.expect("TriggerRule new state", {
          case DamlTuple2(SUnit, newState) =>
            logger.debug(s"New state: $newState")
            newState
        }).orConverterException)
    }

    val logInitialState =
      Flow[SValue].wireTap(evaluatedInitialState =>
        logger.debug(s"Initial state: $evaluatedInitialState"))

    // The flow that we return:
    //  - Maps incoming trigger messages to new trigger messages
    //    replacing ledger command IDs with the IDs used internally;
    //  - Folds over the trigger messages via the speedy machine
    //    thereby accumulating the state changes.
    // The materialized value of the flow is the (future) final state
    // of this process.
    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    val graph = GraphDSL.create(Sink.last[SValue]) { implicit gb => saveLastState =>
      import GraphDSL.Implicits._
      val msgIn = gb add Flow[TriggerMsg].wireTap(logReceivedMsg _)
      val initialState = gb add runInitialState
      val initialStateOut = gb add Broadcast[SValue](2)
      val rule = gb add runRuleOnMsgs
      val submissions = gb add Merge[SubmitRequest](2)
      val finalStateIn = gb add Concat[SValue](2)
      // format: off
      initialState.finalState ~> logInitialState ~> initialStateOut ~> rule.initState
      initialState.elemsOut                          ~> submissions
                          msgIn ~> hideIrrelevantMsgs ~> encodeMsgs ~> rule.elemsIn
                                                        submissions <~ rule.elemsOut
                                                    initialStateOut                     ~> finalStateIn
                                                                       rule.finalStates ~> finalStateIn ~> saveLastState
      // format: on
      new FlowShape(msgIn.in, submissions.out)
    }
    Flow fromGraph graph
  }

  private[this] def hideIrrelevantMsgs: Flow[TriggerMsg, TriggerMsg, NotUsed] =
    Flow[TriggerMsg].mapConcat[TriggerMsg] {
      case msg @ CompletionMsg(c) =>
        // This happens for invalid UUIDs which we might get for
        // completions not emitted by the trigger.
        val ouuid = fromTryCatchThrowable[UUID, IllegalArgumentException](
          UUID.fromString(c.commandId)).toOption
        ouuid.flatMap { uuid =>
          useCommandId(uuid, SeenMsgs.Completion) option msg
        }.toList
      case msg @ TransactionMsg(t) =>
        // This happens for invalid UUIDs which we might get for
        // transactions not emitted by the trigger.
        val ouuid = fromTryCatchThrowable[UUID, IllegalArgumentException](
          UUID.fromString(t.commandId)).toOption
        List(ouuid flatMap { uuid =>
          useCommandId(uuid, SeenMsgs.Transaction) option msg
        } getOrElse TransactionMsg(t.copy(commandId = "")))
      case x @ HeartbeatMsg() => List(x) // Hearbeats don't carry any information.
    }

  def makeApp(func: SExpr, values: Array[SValue]): SExpr = {
    SEApp(func, values.map(SEValue(_)))
  }

  private[this] def makeAppD(func: SValue, values: SValue*) = makeApp(SEValue(func), values.toArray)

  // Query the ACS. This allows you to separate the initialization of
  // the initial state from the first run.
  def queryACS()(
      implicit materializer: Materializer,
      executionContext: ExecutionContext): Future[(Seq[CreatedEvent], LedgerOffset)] = {
    for {
      acsResponses <- client.activeContractSetClient
        .getActiveContracts(transactionFilter, verbose = false)
        .runWith(Sink.seq)
      offset = Array(acsResponses: _*).lastOption
        .fold(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))(resp =>
          LedgerOffset().withAbsolute(resp.offset))
    } yield (acsResponses.flatMap(x => x.activeContracts), offset)
  }

  private[this] def submitOrFail(
      implicit ec: ExecutionContext): Flow[SubmitRequest, SingleCommandFailure, NotUsed] = {
    def submit(req: SubmitRequest) = {
      val f: Future[Empty] = client.commandClient
        .submitSingleCommand(req)
      f.map(_ => None).recover {
        case s: StatusRuntimeException if s.getStatus != io.grpc.Status.UNAUTHENTICATED =>
          // Do not capture UNAUTHENTICATED errors.
          // The access token may be expired, let the trigger runner handle token refresh.
          Some(SingleCommandFailure(req.getCommands.commandId, s))
        // any other error will cause the trigger's stream to fail
      }
    }
    import io.grpc.Status.Code, Code.RESOURCE_EXHAUSTED
    def retryableSubmit(req: SubmitRequest) =
      submit(req).map {
        case Some(SingleCommandFailure(_, s))
            if (s.getStatus.getCode: Code) == (RESOURCE_EXHAUSTED: Code) =>
          None
        case z => Some(z)
      }
    retrying(
      initialTries = maxTriesWhenOverloaded,
      backoff = overloadedRetryDelay,
      parallelism = maxParallelSubmissionsPerTrigger,
      retryableSubmit,
      submit)
      .collect { case Some(err) => err }
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
      executionContext: ExecutionContext): (T, Future[SValue]) = {
    val source =
      msgSource(client, offset, trigger.heartbeat, party, transactionFilter)
    Flow
      .fromGraph(msgFlow)
      .viaMat(getTriggerEvaluator(name, acs))(Keep.both)
      .via(submitOrFail)
      .join(source)
      .run()
  }
}

object Runner extends StrictLogging {

  /** The number of submitSingleCommand invocations each trigger will
    * attempt to execute in parallel.  Note that this does not in any
    * way bound the number of already-submitted, but not completed,
    * commands that may be pending.
    */
  val maxParallelSubmissionsPerTrigger = 8
  val maxTriesWhenOverloaded = 6

  private def overloadedRetryDelay(afterTries: Int): FiniteDuration =
    (250 * (1 << (afterTries - 1))).milliseconds

  // Return the time provider for a given time provider type.
  def getTimeProvider(ty: TimeProviderType): TimeProvider = {
    ty match {
      case TimeProviderType.Static => TimeProvider.Constant(Instant.EPOCH)
      case TimeProviderType.WallClock => TimeProvider.UTC
      case _ => throw new RuntimeException(s"Unexpected TimeProviderType: $ty")
    }
  }

  private object DamlFun {
    def unapply(v: SPAP): Some[SPAP] = Some(v)
  }

  /** Like `CommandRetryFlow` but with no notion of ledger time, and with
    * delay support.  Note that only the future succeeding with `None`
    * indicates that a retry should be attempted; a failed future propagates
    * to the stream.
    */
  private[trigger] def retrying[A, B](
      initialTries: Int,
      backoff: Int => FiniteDuration,
      parallelism: Int,
      retryable: A => Future[Option[B]],
      notRetryable: A => Future[B])(implicit ec: ExecutionContext): Flow[A, B, NotUsed] = {
    def trial(tries: Int, value: A): Future[B] =
      if (tries <= 1) notRetryable(value)
      else
        retryable(value).flatMap(_ cata (Future.successful, {
          Future {
            try Thread.sleep(backoff(initialTries - tries + 1).toMillis)
            catch { case _: InterruptedException => }
          }.flatMap(_ => trial(tries - 1, value))
        }))
    Flow[A].mapAsync(parallelism)(trial(initialTries, _))
  }

  private def alterF[K, V, F[a] >: Option[a]: Functor](m: Map[K, V], k: K)(
      f: Option[V] => F[Option[V]]): F[Map[K, V]] = {
    val ov = m get k
    f(ov) map {
      (ov, _) match {
        case (_, Some(v)) => m updated (k, v)
        case (None, None) => m
        case (Some(_), None) => m - k
      }
    }
  }

  private final case class SingleCommandFailure(commandId: String, s: StatusRuntimeException)

  private sealed abstract class SeenMsgs {
    import Runner.{SeenMsgs => S}
    def see(msg: S.One): Option[SeenMsgs] =
      (this, msg).match2 {
        case S.Completion => { case S.Transaction => None }
        case S.Transaction => { case S.Completion => None }
        case S.Neither => { case one => Some(one: SeenMsgs) }
      }(fallback = Some(this))
  }

  private object SeenMsgs {
    sealed abstract class One extends SeenMsgs
    case object Completion extends One
    case object Transaction extends One
    case object Neither extends SeenMsgs
  }

  // Convience wrapper that creates the runner and runs the trigger.
  def run(
      dar: Dar[(PackageId, Package)],
      triggerId: Identifier,
      client: LedgerClient,
      timeProviderType: TimeProviderType,
      applicationId: ApplicationId,
      party: String
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[SValue] = {
    val darMap = dar.all.toMap
    val compiledPackages = PureCompiledPackages(darMap).right.get
    val trigger = Trigger.fromIdentifier(compiledPackages, triggerId) match {
      case Left(err) => throw new RuntimeException(s"Invalid trigger: $err")
      case Right(trigger) => trigger
    }
    val runner =
      newLoggingContext(label[Trigger], trigger.loggingExtension) { implicit lc =>
        new Runner(compiledPackages, trigger, client, timeProviderType, applicationId, party)
      }
    for {
      (acs, offset) <- runner.queryACS()
      finalState <- runner.runWithACS(acs, offset)._2
    } yield finalState
  }
}
