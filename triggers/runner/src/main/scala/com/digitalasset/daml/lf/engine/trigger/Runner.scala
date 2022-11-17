// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl._
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.{
  Command,
  Commands,
  CreateAndExerciseCommand,
  CreateCommand,
  ExerciseByKeyCommand,
  ExerciseCommand,
}
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  InterfaceFilter,
  TransactionFilter,
}
import com.daml.ledger.api.v1.{value => api}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.services.commands.CompletionStreamElement._
import com.daml.lf.archive.Dar
import com.daml.lf.data.FrontStack
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref._
import com.daml.lf.data.ScalazEqual._
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.trigger.Runner.Implicits._
import com.daml.lf.engine.trigger.Runner.{TriggerContext, logger}
import com.daml.lf.language.Ast._
import com.daml.lf.language.PackageInterface
import com.daml.lf.language.Util._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{Compiler, Pretty, SValue, Speedy}
import com.daml.lf.{CompiledPackages, PureCompiledPackages}
import com.daml.logging.LoggingContextOf.label
import com.daml.logging.entries.{LoggingEntries, LoggingEntry, LoggingValue, ToLoggingValue}
import com.daml.logging.entries.ToLoggingValue._
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.platform.participant.util.LfEngineToApi.toApiIdentifier
import com.daml.platform.services.time.TimeProviderType
import com.daml.scalautil.Statement.discard
import com.daml.script.converter.Converter.Implicits._
import com.daml.script.converter.Converter.{DamlAnyModuleRecord, DamlTuple2, unrollFree}
import com.daml.script.converter.ConverterException
import com.daml.util.Ctx
import com.google.protobuf.empty.Empty
import com.google.rpc.status.Status
import io.grpc.Status.UNAVAILABLE
import io.grpc.StatusRuntimeException
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.syntax.tag._
import scalaz.{-\/, Tag, \/, \/-}

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.{nowarn, tailrec}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

private[lf] sealed trait TriggerMsg
private[lf] object TriggerMsg {
  final case class Completion(c: com.daml.ledger.api.v1.completion.Completion) extends TriggerMsg
  final case class Transaction(t: com.daml.ledger.api.v1.transaction.Transaction) extends TriggerMsg
  final case object Heartbeat extends TriggerMsg
}

private[lf] final case class TriggerDefinition(
    id: Identifier,
    ty: TypeConApp,
    version: Trigger.Version,
    level: Trigger.Level,
    expr: Expr,
) {
  val triggerIds = TriggerIds(ty.tycon.packageId)
}

private[lf] final case class Trigger(
    defn: TriggerDefinition,
    filters: Filters, // We store Filters rather than
    // TransactionFilter since the latter is
    // party-specific.
    heartbeat: Option[FiniteDuration],
    // Whether the trigger supports readAs claims (SDK 1.18 and newer) or not.
    hasReadAs: Boolean,
)

// Utilities for interacting with the speedy machine.
private[lf] object Machine {

  // Run speedy until we arrive at a value.
  def stepToValue(
      machine: Speedy.OffLedgerMachine
  )(implicit loggingContext: LoggingContextOf[Trigger]): SValue = {
    machine.run() match {
      case SResultFinal(v) => v
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

object Trigger {

  private[trigger] def newLoggingContext[P, T](
      triggerDefinition: Identifier,
      actAs: Party,
      readAs: Set[Party],
      triggerId: String,
      applicationId: ApplicationId,
  ): (LoggingContextOf[Trigger with P] => T) => T = {
    val entries = List[LoggingEntry](
      "id" -> triggerId,
      "definition" -> triggerDefinition,
      "actAs" -> Tag.unwrap(actAs),
      "readAs" -> Tag.unsubst(readAs),
    )

    LoggingContextOf.newLoggingContext(
      LoggingContextOf.label[Trigger with P],
      "applicationId" -> applicationId.unwrap,
      "trigger" -> LoggingValue.Nested(LoggingEntries(entries: _*)),
    )
  }

  private def detectHasReadAs(
      pkgInterface: PackageInterface,
      triggerIds: TriggerIds,
  ): Either[String, Boolean] =
    for {
      fieldInfo <- pkgInterface
        .lookupRecordFieldInfo(
          triggerIds.damlTriggerLowLevel("Trigger"),
          Name.assertFromString("initialState"),
        )
        .left
        .map(_.pretty)
      hasReadAs <- fieldInfo.typDef match {
        case TFun(TParty, TFun(TList(TParty), TFun(_, _))) => Right(true)
        case TFun(TParty, TFun(_, _)) => Right(false)
        case t => Left(s"Internal error: Unexpected type for initialState function: $t")
      }
    } yield hasReadAs

  private[trigger] def detectVersion(
      pkgInterface: PackageInterface,
      triggerIds: TriggerIds,
  ): Either[String, Trigger.Version] = {
    val versionTypeCon = triggerIds.damlTriggerInternal("Version")
    pkgInterface.lookupDataEnum(versionTypeCon) match {
      case Left(_) =>
        Version.fromString(None)
      case Right(enum) =>
        enum.dataEnum.constructors match {
          case ImmArray(versionCons) =>
            Version.fromString(Some(versionCons))
          case _ =>
            Left("can not infer trigger version")
        }
    }
  }

  // Given an identifier to a high- or lowlevel trigger,
  // return an expression that will run the corresponding trigger
  // as a low-level trigger (by applying runTrigger) and the type of that expression.
  private[trigger] def detectTriggerDefinition(
      pkgInterface: PackageInterface,
      triggerId: Identifier,
  ): Either[String, TriggerDefinition] = {

    def error(triggerId: Identifier, ty: Type): Left[String, Nothing] = {
      val triggerIds = TriggerIds(Ref.PackageId.assertFromString("-"))
      val highLevelTrigger = TTyCon(triggerIds.damlTrigger("Trigger"))
      val lowLevelTrigger = TTyCon(triggerIds.damlTriggerLowLevel("Trigger"))
      val a = TVar(Name.assertFromString("a"))
      Left(
        s"the definition $triggerId does not have valid trigger type: " +
          "expected a type of the form " +
          s"(${TApp(highLevelTrigger, a).pretty}) or (${TApp(lowLevelTrigger, a).pretty}) " +
          s"but got (${ty.pretty})"
      )
    }

    pkgInterface.lookupValue(triggerId) match {
      case Right(DValueSignature(TApp(ty @ TTyCon(tyCon), tyArg), _, _)) =>
        val triggerIds = TriggerIds(tyCon.packageId)
        if (tyCon == triggerIds.damlTriggerLowLevel("Trigger")) {
          val expr = EVal(triggerId)
          val ty = TypeConApp(tyCon, ImmArray(tyArg))
          detectVersion(pkgInterface, triggerIds).map(
            TriggerDefinition(triggerId, ty, _, Level.Low, expr)
          )
        } else if (tyCon == triggerIds.damlTrigger("Trigger")) {
          val runTrigger = EVal(triggerIds.damlTrigger("runTrigger"))
          val expr = EApp(runTrigger, EVal(triggerId))
          val triggerState = TTyCon(triggerIds.damlTriggerInternal("TriggerState"))
          val stateTy = TApp(triggerState, tyArg)
          val lowLevelTriggerTy = triggerIds.damlTriggerLowLevel("Trigger")
          val ty = TypeConApp(lowLevelTriggerTy, ImmArray(stateTy))
          detectVersion(pkgInterface, triggerIds).map(
            TriggerDefinition(triggerId, ty, _, Level.High, expr)
          )
        } else {
          error(triggerId, ty)
        }

      case Right(DValueSignature(ty, _, _)) =>
        error(triggerId, ty)

      case Left(err) =>
        Left(err.pretty)
    }
  }

  def fromIdentifier(
      compiledPackages: CompiledPackages,
      triggerId: Identifier,
  )(implicit loggingContext: LoggingContextOf[Trigger]): Either[String, Trigger] = {
    val compiler = compiledPackages.compiler

    for {
      triggerDef <- detectTriggerDefinition(compiledPackages.pkgInterface, triggerId)
      hasReadAs <- detectHasReadAs(compiledPackages.pkgInterface, triggerDef.triggerIds)
      converter = new Converter(compiledPackages, triggerDef)
      filter <- getTriggerFilter(compiledPackages, compiler, converter, triggerDef)
      heartbeat <- getTriggerHeartbeat(compiledPackages, compiler, converter, triggerDef)
    } yield Trigger(triggerDef, filter, heartbeat, hasReadAs)
  }

  // Return the heartbeat specified by the user.
  private def getTriggerHeartbeat(
      compiledPackages: CompiledPackages,
      compiler: Compiler,
      converter: Converter,
      triggerDef: TriggerDefinition,
  )(implicit loggingContext: LoggingContextOf[Trigger]): Either[String, Option[FiniteDuration]] = {
    val heartbeat = compiler.unsafeCompile(
      ERecProj(triggerDef.ty, Name.assertFromString("heartbeat"), triggerDef.expr)
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
      triggerDef: TriggerDefinition,
  )(implicit loggingContext: LoggingContextOf[Trigger]): Either[String, Filters] = {
    val registeredTemplates = compiler.unsafeCompile(
      ERecProj(triggerDef.ty, Name.assertFromString("registeredTemplates"), triggerDef.expr)
    )
    val machine = Speedy.Machine.fromPureSExpr(compiledPackages, registeredTemplates)
    val packages = compiledPackages.packageIds
      .map(pkgId => (pkgId, compiledPackages.pkgInterface.lookupPackage(pkgId).toOption.get))
      .toSeq
    def templateFilter(isRegistered: Identifier => Boolean) =
      packages.flatMap({ case (pkgId, pkg) =>
        pkg.modules.toList.flatMap({ case (modName, module) =>
          module.templates.keys.collect {
            case entityName
                if isRegistered(Identifier(pkgId, QualifiedName(modName, entityName))) =>
              toApiIdentifier(Identifier(pkgId, QualifiedName(modName, entityName)))
          }
        })
      })
    def interfaceFilter(isRegistered: Identifier => Boolean) =
      packages.flatMap({ case (pkgId, pkg) =>
        pkg.modules.toList.flatMap({ case (modName, module) =>
          module.interfaces.keys.collect {
            case entityName
                if isRegistered(Identifier(pkgId, QualifiedName(modName, entityName))) =>
              InterfaceFilter(
                interfaceId =
                  Some(toApiIdentifier(Identifier(pkgId, QualifiedName(modName, entityName)))),
                includeInterfaceView = true,
                includeCreateArgumentsBlob = false,
              )
          }
        })
      })

    Machine.stepToValue(machine) match {
      case SVariant(_, "AllInDar", _, _) =>
        Right(
          Filters(
            Some(
              InclusiveFilters(
                templateIds = templateFilter(_ => true),
                interfaceFilters = interfaceFilter(_ => true),
              )
            )
          )
        )

      case SVariant(_, "RegisteredTemplates", _, v) =>
        converter.toRegisteredTemplates(v) match {
          case Right(identifiers) =>
            val isRegistered: Identifier => Boolean = identifiers.toSet.contains
            Right(
              Filters(
                Some(
                  InclusiveFilters(
                    templateIds = templateFilter(isRegistered),
                    interfaceFilters = interfaceFilter(isRegistered),
                  )
                )
              )
            )

          case Left(err) =>
            Left(err)
        }

      case v =>
        Left(s"Expected AllInDar or RegisteredTemplates but got $v")
    }
  }

  sealed abstract class Version extends Serializable
  object Version {
    case object `2.0` extends Version
    case object `2.5` extends Version

    def fromString(s: Option[String]): Either[String, Version] =
      s match {
        case None => Right(`2.0`)
        case Some("Version_2_5") => Right(`2.5`)
        case Some(s) => Left(s"""cannot parse trigger version "$s".""")
      }
  }

  sealed abstract class Level extends Serializable
  object Level {
    case object Low extends Level
    case object High extends Level
  }
}

private[lf] class Runner private (
    compiledPackages: CompiledPackages,
    trigger: Trigger,
    triggerConfig: TriggerRunnerConfig,
    client: LedgerClient,
    timeProviderType: TimeProviderType,
    applicationId: ApplicationId,
    parties: TriggerParties,
)(implicit loggingContext: LoggingContextOf[Trigger]) {
  import Runner.SeenMsgs

  // Compiles LF expressions into Speedy expressions.
  private val compiler = compiledPackages.compiler
  // Converts between various objects and SValues.
  private val converter: Converter = new Converter(compiledPackages, trigger.defn)

  private class InFlightCommands {
    // These are the command IDs used on the ledger API to submit commands for
    // this trigger for which we are awaiting either a completion or transaction
    // message, or both.
    // This is a data structure that is shared across (potentially) multiple async contexts
    // - hence why we use a scala.concurrent.TrieMap here.
    private[this] val pendingIds = TrieMap.empty[UUID, SeenMsgs]
    // Due to concurrency, inFlight counts are eventually consistent (with pendingIds) and so only
    // approximate the actual commands that are in-flight.
    private[this] val inFlight: AtomicLong = new AtomicLong(0)

    def get(uuid: UUID): Option[SeenMsgs] = {
      pendingIds.get(uuid)
    }

    def count: Int = {
      inFlight.intValue()
    }

    def update(uuid: UUID, seeOne: SeenMsgs)(implicit
        loggingContext: LoggingContextOf[Trigger]
    ): Unit = {
      val inFlightState = pendingIds.put(uuid, seeOne)

      (inFlightState, seeOne) match {
        case (None, SeenMsgs.Neither) =>
          logger.info("New in-flight command")
          discard(inFlight.incrementAndGet())

        case (None, _) | (Some(SeenMsgs.Neither), SeenMsgs.Neither) =>
        // no work required

        case (Some(SeenMsgs.Neither), _) =>
          logger.info("In-flight command completed")
          discard(inFlight.decrementAndGet())

        case (Some(_), SeenMsgs.Neither) =>
          logger.info("New in-flight command")
          discard(inFlight.incrementAndGet())

        case (Some(_), _) =>
        // no work required
      }
    }

    def remove(uuid: UUID)(implicit loggingContext: LoggingContextOf[Trigger]): Unit = {
      val inFlightState = pendingIds.remove(uuid)

      if (inFlightState.contains(SeenMsgs.Neither)) {
        logger.info("In-flight command completed")
        discard(inFlight.decrementAndGet())
      }
    }
  }

  private[this] val inFlightCommands = new InFlightCommands

  private val transactionFilter =
    TransactionFilter(parties.readers.map(p => (p.unwrap, trigger.filters)).toMap)

  // return whether uuid *was* present in pendingCommandIds
  private[this] def useCommandId(uuid: UUID, seeOne: SeenMsgs.One)(implicit
      loggingContext: LoggingContextOf[Trigger]
  ): Boolean = {
    loggingContext.withEnrichedTriggerContext(
      "commandId" -> uuid
    ) { implicit loggingContext: LoggingContextOf[Trigger] =>
      inFlightCommands.get(uuid) match {
        case None =>
          false

        case Some(seenOne) =>
          seenOne.see(seeOne) match {
            case Some(v) =>
              inFlightCommands.update(uuid, v)
            case None =>
              inFlightCommands.remove(uuid)
          }
          true
      }
    }
  }

  import Runner.{DamlFun, SingleCommandFailure, overloadedRetryDelay, retrying}

  @throws[RuntimeException]
  private def handleCommands(
      commands: Seq[Command]
  )(implicit loggingContext: LoggingContextOf[Trigger]): (UUID, SubmitRequest) = {
    val commandUUID = UUID.randomUUID

    loggingContext.withEnrichedTriggerContext(
      "commandId" -> commandUUID,
      "commands" -> commands,
    ) { implicit loggingContext: LoggingContextOf[Trigger] =>
      inFlightCommands.update(commandUUID, SeenMsgs.Neither)

      val commandsArg = Commands(
        ledgerId = client.ledgerId.unwrap,
        applicationId = applicationId.unwrap,
        commandId = commandUUID.toString,
        party = parties.actAs.unwrap,
        readAs = Party.unsubst(parties.readAs).toList,
        commands = commands,
      )

      (commandUUID, SubmitRequest(commands = Some(commandsArg)))
    }
  }

  private def freeTriggerSubmits(
      clientTime: Timestamp,
      v: SValue,
  )(implicit
      loggingContext: LoggingContextOf[Trigger]
  ): UnfoldState[SValue, TriggerContext[SubmitRequest]] = {
    def evaluate(se: SExpr): SValue = {
      val machine: Speedy.OffLedgerMachine =
        Speedy.Machine.fromPureSExpr(compiledPackages, se)
      // Evaluate it.
      machine.setExpressionToEvaluate(se)
      Machine.stepToValue(machine)
    }
    type Termination = SValue \/ (TriggerContext[SubmitRequest], SValue)
    @tailrec def go(v: SValue): Termination = {
      val resumed: Termination Either SValue = unrollFree(v) match {
        case Right(Right(vvv @ (variant, vv))) =>
          // Must be kept in-sync with the DAML code LowLevel#TriggerF
          vvv.match2 {
            case "GetTime" /*(Time -> a)*/ => { case DamlFun(timeA) =>
              Right(evaluate(makeAppD(timeA, STimestamp(clientTime))))
            }
            case "Submit" /*([Command], Text -> a)*/ => {
              case DamlTuple2(sCommands, DamlFun(textA)) =>
                val commands = converter.toCommands(sCommands).orConverterException
                val (commandUUID, submitRequest) = handleCommands(commands)
                Left(
                  \/-(
                    (
                      Ctx(loggingContext, submitRequest),
                      evaluate(makeAppD(textA, SText((commandUUID: UUID).toString))),
                    )
                  ): Termination
                )
            }
            case _ =>
              logger.error("Unrecognised TriggerF step")(
                loggingContext.enrichTriggerContext("variant" -> variant)
              )
              throw new ConverterException(s"unrecognized TriggerF step $variant")
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
      parties: TriggerParties,
      filter: TransactionFilter,
  ): Flow[TriggerContext[SingleCommandFailure], TriggerContext[TriggerMsg], NotUsed] = {
    // A queue for command submission failures.
    val submissionFailureQueue
        : Flow[TriggerContext[SingleCommandFailure], TriggerContext[TriggerMsg], NotUsed] = {
      Flow[TriggerContext[SingleCommandFailure]]
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
        .buffer(triggerConfig.submissionFailureQueueSize, OverflowStrategy.fail)
        .map {
          _.map { case SingleCommandFailure(commandId, s) =>
            TriggerMsg.Completion(
              Completion(
                commandId,
                Some(Status(s.getStatus.getCode.value(), s.getStatus.getDescription)),
              )
            )
          }
        }
    }

    // The transaction source (ledger).
    val transactionSource: Source[TriggerContext[TriggerMsg], NotUsed] = {
      logger.info("Subscribing to ledger API transaction source")(
        loggingContext.enrichTriggerContext("filter" -> filter)
      )
      client.transactionClient
        .getTransactions(offset, None, filter)
        .map { transaction =>
          Ctx(loggingContext, TriggerMsg.Transaction(transaction))
        }
    }

    // Command completion source (ledger completion stream)
    val completionSource: Source[TriggerContext[TriggerMsg], NotUsed] = {
      logger.info("Subscribing to ledger API completion source")
      client.commandClient
        // Completions only take actAs into account so no need to include readAs.
        .completionSource(List(parties.actAs.unwrap), offset)
        .collect { case CompletionElement(completion, _) =>
          Ctx(loggingContext, TriggerMsg.Completion(completion))
        }
    }

    // Heartbeats source (we produce these repetitively on a timer with
    // the given delay interval).
    val heartbeatSource: Source[TriggerContext[TriggerMsg], NotUsed] = heartbeat match {
      case Some(interval) =>
        logger.info("Heartbeat source configured")(
          loggingContext.enrichTriggerContext("heartbeat" -> interval)
        )
        Source
          .tick[TriggerMsg](interval, interval, TriggerMsg.Heartbeat)
          .mapMaterializedValue(_ => NotUsed)
          .map { heartbeat =>
            Ctx(loggingContext, heartbeat)
          }

      case None =>
        logger.info("No heartbeat source configured")
        Source.empty[TriggerContext[TriggerMsg]]
    }

    submissionFailureQueue
      .merge(completionSource merge transactionSource merge heartbeatSource)
  }

  private[this] def getInitialStateFreeAndUpdate(acs: Seq[CreatedEvent]): (SValue, SValue) = {
    // Compile the trigger initialState and Update LF functions to
    // speedy expressions.
    val update: SExpr =
      compiler.unsafeCompile(
        ERecProj(trigger.defn.ty, Name.assertFromString("update"), trigger.defn.expr)
      )
    val getInitialState: SExpr =
      compiler.unsafeCompile(
        ERecProj(trigger.defn.ty, Name.assertFromString("initialState"), trigger.defn.expr)
      )
    // Convert the ACS to a speedy value.
    val createdValue: SValue = converter.fromACS(acs).orConverterException
    // Setup an application expression of initialState on the ACS.
    val partyArg = SParty(Ref.Party.assertFromString(parties.actAs.unwrap))
    val initialStateArgs = if (trigger.hasReadAs) {
      val readAsArg = SList(
        parties.readAs.map(p => SParty(Ref.Party.assertFromString(p.unwrap))).to(FrontStack)
      )
      Array(partyArg, readAsArg, createdValue)
    } else Array(partyArg, createdValue)
    val initialState: SExpr =
      makeApp(
        getInitialState,
        initialStateArgs,
      )
    // Prepare a speedy machine for evaluating expressions.
    val machine: Speedy.OffLedgerMachine =
      Speedy.Machine.fromPureSExpr(compiledPackages, initialState)
    // Evaluate it.
    machine.setExpressionToEvaluate(initialState)
    val initialStateFree = Machine
      .stepToValue(machine)
      .expect(
        "TriggerSetup",
        { case DamlAnyModuleRecord("TriggerSetup", fts) => fts }: @nowarn(
          "msg=A repeated case parameter or extracted sequence is not matched by a sequence wildcard"
        ),
      )
      .orConverterException
    machine.setExpressionToEvaluate(update)
    val evaluatedUpdate: SValue = Machine.stepToValue(machine)
    (initialStateFree, evaluatedUpdate)
  }

  private[this] def encodeMsgs: Flow[TriggerContext[TriggerMsg], TriggerContext[SValue], NotUsed] =
    Flow fromFunction {
      case ctx @ Ctx(_, TriggerMsg.Transaction(transaction), _) =>
        ctx.copy(value = converter.fromTransaction(transaction).orConverterException)

      case ctx @ Ctx(_, TriggerMsg.Completion(completion), _) =>
        ctx.copy(value = converter.fromCompletion(completion).orConverterException)

      case ctx @ Ctx(_, TriggerMsg.Heartbeat, _) =>
        ctx.copy(value = converter.fromHeartbeat)
    }

  // A flow for trigger messages representing a process for the
  // accumulated state changes resulting from application of the
  // messages given the starting state represented by the ACS
  // argument.
  private def getTriggerEvaluator(
      acs: Seq[CreatedEvent]
  ): Flow[TriggerContext[TriggerMsg], TriggerContext[SubmitRequest], Future[SValue]] = {
    logger.info("Trigger starting")

    val clientTime: Timestamp =
      Timestamp.assertFromInstant(Runner.getTimeProvider(timeProviderType).getCurrentTime)
    val (initialStateFree, evaluatedUpdate) = getInitialStateFreeAndUpdate(acs)
    // Prepare another speedy machine for evaluating expressions.
    val machine: Speedy.OffLedgerMachine =
      Speedy.Machine.fromPureSExpr(compiledPackages, SEValue(SUnit))

    import UnfoldState.{flatMapConcatNode, flatMapConcatNodeOps, toSource, toSourceOps}

    val runInitialState = toSource(freeTriggerSubmits(clientTime, initialStateFree))

    val runRuleOnMsgs = flatMapConcatNode { (state: SValue, messageVal: TriggerContext[SValue]) =>
      val clientTime: Timestamp =
        Timestamp.assertFromInstant(Runner.getTimeProvider(timeProviderType).getCurrentTime)
      machine.setExpressionToEvaluate(makeAppD(evaluatedUpdate, messageVal.value))
      val stateFun = Machine
        .stepToValue(machine)
        .expect(
          "TriggerRule",
          { case DamlAnyModuleRecord("TriggerRule", DamlAnyModuleRecord("StateT", fun)) =>
            fun
          }: @nowarn("msg=A repeated case parameter .* is not matched by a sequence wildcard"),
        )
        .orConverterException
      machine.setExpressionToEvaluate(makeAppD(stateFun, state))
      val updateWithNewState = Machine.stepToValue(machine)

      freeTriggerSubmits(clientTime, v = updateWithNewState)
        .leftMap(
          _.expect(
            "TriggerRule new state",
            { case DamlTuple2(SUnit, newState) =>
              logger.debug(s"New state: $newState")
              newState
            },
          ).orConverterException
        )
    }

    // The flow that we return:
    //  - Maps incoming trigger messages to new trigger messages
    //    replacing ledger command IDs with the IDs used internally;
    //  - Folds over the trigger messages via the speedy machine
    //    thereby accumulating the state changes.
    // The materialized value of the flow is the (future) final state
    // of this process.
    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    val graph = GraphDSL.createGraph(Sink.last[SValue]) { implicit gb => saveLastState =>
      import GraphDSL.Implicits._

      val msgIn = gb add Flow[TriggerContext[TriggerMsg]]
      val initialState = gb add runInitialState
      val initialStateOut = gb add Broadcast[SValue](2)
      val rule = gb add runRuleOnMsgs
      val submissions = gb add Merge[TriggerContext[SubmitRequest]](2)
      val finalStateIn = gb add Concat[SValue](2)
      // format: off
      initialState.finalState                    ~> initialStateOut ~> rule.initState
      initialState.elemsOut                          ~> submissions
                          msgIn ~> hideIrrelevantMsgs ~> encodeMsgs ~> rule.elemsIn
                                                        submissions <~ rule.elemsOut
                                                    initialStateOut                     ~> finalStateIn
                                                                       rule.finalStates ~> finalStateIn ~> saveLastState
      // format: on
      new FlowShape(msgIn.in, submissions.out)
    }

    Flow.fromGraph(graph)
  }

  private[this] def catchIAE[A](a: => A): Option[A] =
    try Some(a)
    catch { case _: IllegalArgumentException => None }

  private[this] def hideIrrelevantMsgs
      : Flow[TriggerContext[TriggerMsg], TriggerContext[TriggerMsg], NotUsed] =
    Flow[TriggerContext[TriggerMsg]].mapConcat[TriggerContext[TriggerMsg]] {
      case ctx @ Ctx(_, msg @ TriggerMsg.Completion(c), _) =>
        // This happens for invalid UUIDs which we might get for
        // completions not emitted by the trigger.
        val ouuid = catchIAE(UUID.fromString(c.commandId))
        ouuid.flatMap { uuid =>
          useCommandId(uuid, SeenMsgs.Completion)(ctx.context) option ctx.copy(value = msg)
        }.toList

      case ctx @ Ctx(_, msg @ TriggerMsg.Transaction(t), _) =>
        // This happens for invalid UUIDs which we might get for
        // transactions not emitted by the trigger.
        val ouuid = catchIAE(UUID.fromString(t.commandId))
        List(ouuid flatMap { uuid =>
          useCommandId(uuid, SeenMsgs.Transaction)(ctx.context) option ctx.copy(value = msg)
        } getOrElse ctx.copy(value = TriggerMsg.Transaction(t.copy(commandId = ""))))

      case ctx @ Ctx(_, msg @ TriggerMsg.Heartbeat, _) =>
        List(ctx.copy(value = msg)) // Heartbeats don't carry any information.
    }

  def makeApp(func: SExpr, values: Array[SValue]): SExpr = {
    SEApp(func, values)
  }

  private[this] def makeAppD(func: SValue, values: SValue*): SExpr =
    makeApp(SEValue(func), values.toArray)

  // Query the ACS. This allows you to separate the initialization of
  // the initial state from the first run.
  def queryACS()(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): Future[(Seq[CreatedEvent], LedgerOffset)] = {
    for {
      acsResponses <- client.activeContractSetClient
        .getActiveContracts(transactionFilter)
        .runWith(Sink.seq)
      offset = Array(acsResponses: _*).lastOption
        .fold(LedgerOffset().withBoundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))(resp =>
          LedgerOffset().withAbsolute(resp.offset)
        )
    } yield (acsResponses.flatMap(x => x.activeContracts), offset)
  }

  private[this] def submitOrFail(implicit
      ec: ExecutionContext
  ): Flow[TriggerContext[SubmitRequest], TriggerContext[SingleCommandFailure], NotUsed] = {
    import io.grpc.Status.Code
    import Code.RESOURCE_EXHAUSTED

    def submit(req: TriggerContext[SubmitRequest]): Future[Option[SingleCommandFailure]] = {
      val f: Future[Empty] = client.commandClient.submitSingleCommand(req.value)
      f.map(_ => None).recover {
        case s: StatusRuntimeException if s.getStatus.getCode != Code.UNAUTHENTICATED =>
          // Do not capture UNAUTHENTICATED errors.
          // The access token may be expired, let the trigger runner handle token refresh.
          Some(SingleCommandFailure(req.value.getCommands.commandId, s))
        // any other error will cause the trigger's stream to fail
      }
    }

    def retryableSubmit(
        req: TriggerContext[SubmitRequest]
    ): Future[Option[Option[SingleCommandFailure]]] =
      submit(req).map {
        case Some(SingleCommandFailure(_, s))
            if (s.getStatus.getCode: Code) == (RESOURCE_EXHAUSTED: Code) =>
          None
        case z => Some(z)
      }

    val throttleFlow: Flow[TriggerContext[SubmitRequest], TriggerContext[SubmitRequest], NotUsed] =
      Flow[TriggerContext[SubmitRequest]]
        .throttle(triggerConfig.maxSubmissionRequests, triggerConfig.maxSubmissionDuration)
    val submitFlow
        : Flow[TriggerContext[SubmitRequest], TriggerContext[SingleCommandFailure], NotUsed] =
      Flow[TriggerContext[SubmitRequest]]
        .filter { _ =>
          inFlightCommands.count <= triggerConfig.maxInFlightCommands
        }
        .via(
          retrying(
            initialTries = triggerConfig.maxRetries,
            backoff = overloadedRetryDelay,
            parallelism = triggerConfig.parallelism,
            retryableSubmit,
            submit,
          )
            .collect { case ctx @ Ctx(_, Some(err), _) =>
              logger.warn("Ledger API command submission request failed")(
                ctx.context.enrichTriggerContext("failure" -> err)
              )

              ctx.copy(value = err)
            }
        )
    val failureFlow
        : Flow[TriggerContext[SubmitRequest], TriggerContext[SingleCommandFailure], NotUsed] =
      Flow[TriggerContext[SubmitRequest]]
        .filterNot { _ =>
          inFlightCommands.count <= triggerConfig.maxInFlightCommands
        }
        .map { case Ctx(loggingContext, request, _) =>
          logger.warn("Due to excessive in-flight commands, failing submission request")(
            loggingContext.enrichTriggerContext(
              "commandId" -> request.getCommands.commandId,
              "in-flight-commands" -> inFlightCommands.count,
              "max-in-flight-commands" -> triggerConfig.maxInFlightCommands,
            )
          )

          Ctx(
            loggingContext,
            SingleCommandFailure(
              request.getCommands.commandId,
              new StatusRuntimeException(UNAVAILABLE),
            ),
          )
        }
    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    val graph = GraphDSL.create() { implicit gb =>
      import GraphDSL.Implicits._

      val broadcast = gb.add(Broadcast[TriggerContext[SubmitRequest]](2))
      val merge = gb.add(Merge[TriggerContext[SingleCommandFailure]](2))
      val throttle = gb.add(throttleFlow)
      val submit = gb.add(submitFlow)
      val failure = gb.add(failureFlow)

      // format: off
      throttle ~> broadcast ~> submit  ~> merge
                  broadcast ~> failure ~> merge
      // format: on

      FlowShape(throttle.in, merge.out)
    }

    Flow.fromGraph(graph)
  }

  // Run the trigger given the state of the ACS. The msgFlow argument
  // passed from ServiceMain is a kill switch. Other choices are
  // possible e.g. 'Flow[TriggerMsg].take()' and this fact is made use
  // of in the test-suite.
  def runWithACS[T](
      acs: Seq[CreatedEvent],
      offset: LedgerOffset,
      msgFlow: Graph[FlowShape[TriggerContext[TriggerMsg], TriggerContext[TriggerMsg]], T] =
        Flow[TriggerContext[TriggerMsg]],
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): (T, Future[SValue]) = {
    val source =
      msgSource(client, offset, trigger.heartbeat, parties, transactionFilter)

    Flow
      .fromGraph(msgFlow)
      .viaMat(getTriggerEvaluator(acs))(Keep.both)
      .via(submitOrFail)
      .join(source)
      .run()
  }
}

object Runner {

  import Implicits._

  private[trigger] val logger = ContextualizedLogger.get(getClass)

  type TriggerContext[+Value] = Ctx[LoggingContextOf[Trigger], Value]

  def apply(
      compiledPackages: CompiledPackages,
      trigger: Trigger,
      triggerConfig: TriggerRunnerConfig,
      client: LedgerClient,
      timeProviderType: TimeProviderType,
      applicationId: ApplicationId,
      parties: TriggerParties,
  )(implicit loggingContext: LoggingContextOf[Trigger]): Runner = {
    val setupId: UUID = UUID.randomUUID()

    loggingContext.withEnrichedTriggerContext(
      triggerAction("trigger.setup", id = setupId)
    ) { implicit loggingContext: LoggingContextOf[Trigger] =>
      loggingContext
        .withEnrichedTriggerContext(
          "level" -> trigger.defn.level.toString,
          "version" -> trigger.defn.version.toString,
        ) { implicit loggingContext: LoggingContextOf[Trigger] =>
          new Runner(
            compiledPackages,
            trigger,
            triggerConfig,
            client,
            timeProviderType,
            applicationId,
            parties,
          )
        }
    }
  }

  def triggerAction(
      name: String,
      id: UUID = UUID.randomUUID(),
      parent: Option[UUID] = None,
  ): (String, LoggingValue) = {
    "action" -> LoggingValue.Nested(
      LoggingEntries(
        "type" -> name,
        "id" -> id,
      ) ++ parent.fold(LoggingEntries.empty)(id => LoggingEntries("parentId" -> id))
    )
  }

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
      retryable: TriggerContext[A] => Future[Option[B]],
      notRetryable: TriggerContext[A] => Future[B],
  )(implicit ec: ExecutionContext): Flow[TriggerContext[A], TriggerContext[B], NotUsed] = {
    def trial(tries: Int, value: TriggerContext[A]): Future[B] = {
      value.context.withEnrichedTriggerContext(
        "max-retries" -> initialTries
      ) { implicit loggingContext: LoggingContextOf[Trigger] =>
        if (tries <= 1) {
          notRetryable(value).recoverWith { case error: Throwable =>
            logger.error("Failing permanently after exhausting submission retries")(
              loggingContext.enrichTriggerContext(
                "attempt" -> initialTries
              )
            )
            Future.failed(error)
          }
        } else {
          retryable(value).flatMap(
            _.cata(
              Future.successful,
              Future {
                val attempt = initialTries - tries + 1
                val backoffPeriod = backoff(attempt)

                logger.warn(
                  "Submission failed, will retry again after backoff period expires"
                )(
                  loggingContext.enrichTriggerContext(
                    "attempt" -> attempt,
                    "backoff-period" -> backoffPeriod,
                  )
                )

                Thread.sleep(backoffPeriod.toMillis)
              }.flatMap(_ => trial(tries - 1, value)),
            )
          )
        }
      }
    }

    Flow[TriggerContext[A]].mapAsync(parallelism) { ctx =>
      logger.info("Submitting request to ledger API")(ctx.context)

      trial(initialTries, ctx).map(b => ctx.copy(value = b))
    }
  }

  private[Runner] final case class SingleCommandFailure(
      commandId: String,
      s: StatusRuntimeException,
  )

  private sealed abstract class SeenMsgs {
    import Runner.{SeenMsgs => S}

    def see(msg: S.One): Option[SeenMsgs] = {
      (this, msg) match {
        case (S.Completion, S.Transaction) | (S.Transaction, S.Completion) =>
          None
        case (S.Neither, _) =>
          Some(msg)
        case _ =>
          Some(this)
      }
    }
  }

  private object SeenMsgs {
    sealed abstract class One extends SeenMsgs
    case object Completion extends One
    case object Transaction extends One
    case object Neither extends SeenMsgs
  }

  // Convenience wrapper that creates the runner and runs the trigger.
  def run(
      dar: Dar[(PackageId, Package)],
      triggerId: Identifier,
      client: LedgerClient,
      timeProviderType: TimeProviderType,
      applicationId: ApplicationId,
      parties: TriggerParties,
      config: Compiler.Config,
      triggerConfig: TriggerRunnerConfig,
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[SValue] =
    Trigger.newLoggingContext(
      triggerId,
      parties.actAs,
      parties.readAs,
      triggerId.toString,
      applicationId,
    ) { implicit loggingContext: LoggingContextOf[Trigger] =>
      val darMap = dar.all.toMap
      val compiledPackages = PureCompiledPackages.build(darMap, config) match {
        case Left(err) => throw new RuntimeException(s"Failed to compile packages: $err")
        case Right(pkgs) => pkgs
      }
      val trigger = Trigger.fromIdentifier(compiledPackages, triggerId) match {
        case Left(err) => throw new RuntimeException(s"Invalid trigger: $err")
        case Right(trigger) => trigger
      }
      val runner =
        Runner(
          compiledPackages,
          trigger,
          triggerConfig,
          client,
          timeProviderType,
          applicationId,
          parties,
        )

      for {
        (acs, offset) <- runner.queryACS()
        finalState <- runner.runWithACS(acs, offset)._2
      } yield finalState
    }

  object Implicits {
    implicit class EnrichTriggerLoggingContextOf(logContext: LoggingContextOf[Trigger]) {
      def enrichTriggerContext(
          entry: LoggingEntry,
          entries: LoggingEntry*
      ): LoggingContextOf[Trigger] = {
        val triggerEntries = logContext.entries.contents.get("trigger") match {
          case Some(LoggingValue.Nested(triggerContext)) =>
            triggerContext ++ LoggingEntries(entry +: entries: _*)

          case _ =>
            LoggingEntries(entry +: entries: _*)
        }

        LoggingContextOf
          .withEnrichedLoggingContext(
            label[Trigger],
            "trigger" -> LoggingValue.Nested(triggerEntries),
          )(logContext)
          .run(identity)
      }

      private[lf] def withEnrichedTriggerContext[A](
          entry: LoggingEntry,
          entries: LoggingEntry*
      )(f: LoggingContextOf[Trigger] => A): A = {
        f(logContext.enrichTriggerContext(entry, entries: _*))
      }
    }

    implicit def `UUID to LoggingValue`: ToLoggingValue[UUID] = ToStringToLoggingValue

    implicit def `api.Identifier to LoggingValue`: ToLoggingValue[api.Identifier] = {
      case api.Identifier(packageId, moduleName, entityName) =>
        s"$moduleName:$entityName@$packageId"
    }

    implicit def `Identifier to LoggingValue`: ToLoggingValue[Identifier] = {
      case Identifier(packageId, qualifiedName) =>
        s"$qualifiedName@$packageId"
    }

    implicit def `FiniteDuration to LoggingValue`: ToLoggingValue[FiniteDuration] =
      ToStringToLoggingValue

    implicit def `SingleCommandFailure to LoggingValue`: ToLoggingValue[SingleCommandFailure] =
      failure =>
        LoggingValue.Nested(
          LoggingEntries(
            "commandId" -> failure.commandId,
            "status" -> failure.s.getStatus.getCode.value(),
            "message" -> failure.s.getStatus.getDescription,
          )
        )

    implicit def `api.Value to LoggingValue`: ToLoggingValue[api.Value] = value =>
      PrettyPrint.prettyApiValue(verbose = true)(value).render(80)

    implicit def `TransactionFilter to LoggingValue`: ToLoggingValue[TransactionFilter] = filter =>
      LoggingValue.Nested(LoggingEntries(filter.filtersByParty.view.mapValues { value =>
        LoggingValue.Nested(
          LoggingEntries(
            "templateIds" -> value.getInclusive.templateIds,
            "interfaceIds" -> value.getInclusive.interfaceFilters.map(_.getInterfaceId),
          )
        )
      }.toSeq: _*))

    implicit def `CreateCommand to LoggingValue`: ToLoggingValue[CreateCommand] = create =>
      LoggingValue.Nested(
        LoggingEntries("type" -> "CreateCommand", "templateId" -> create.getTemplateId)
      )

    implicit def `ExerciseCommand to LoggingValue`: ToLoggingValue[ExerciseCommand] = exercise =>
      LoggingValue.Nested(
        LoggingEntries(
          "type" -> "ExerciseCommand",
          "templateId" -> exercise.getTemplateId,
          "contractId" -> exercise.contractId,
          "choice" -> exercise.choice,
        )
      )

    implicit def `ExerciseByKeyCommand to LoggingValue`: ToLoggingValue[ExerciseByKeyCommand] =
      exerciseByKey =>
        LoggingValue.Nested(
          LoggingEntries(
            "type" -> "ExerciseByKeyCommand",
            "templateId" -> exerciseByKey.getTemplateId,
            "contractKey" -> exerciseByKey.getContractKey,
            "choice" -> exerciseByKey.choice,
          )
        )

    implicit def `CreateAndExerciseCommand to LoggingValue`
        : ToLoggingValue[CreateAndExerciseCommand] = createAndExercise =>
      LoggingValue.Nested(
        LoggingEntries(
          "type" -> "CreateAndExerciseCommand",
          "templateId" -> createAndExercise.getTemplateId,
          "choice" -> createAndExercise.choice,
        )
      )

    implicit def `Command to LoggingValue`: ToLoggingValue[Command] = {
      case Command(Command.Command.Empty) =>
        LoggingValue.Nested(LoggingEntries("type" -> "EmptyCommand"))
      case Command(Command.Command.Create(cmd)) =>
        `CreateCommand to LoggingValue`.toLoggingValue(cmd)
      case Command(Command.Command.Exercise(cmd)) =>
        `ExerciseCommand to LoggingValue`.toLoggingValue(cmd)
      case Command(Command.Command.ExerciseByKey(cmd)) =>
        `ExerciseByKeyCommand to LoggingValue`.toLoggingValue(cmd)
      case Command(Command.Command.CreateAndExercise(cmd)) =>
        `CreateAndExerciseCommand to LoggingValue`.toLoggingValue(cmd)
    }
  }
}
