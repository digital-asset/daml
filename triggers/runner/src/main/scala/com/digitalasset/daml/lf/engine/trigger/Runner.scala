// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.Cancellable
import org.apache.pekko.event.Logging
import org.apache.pekko.stream._
import org.apache.pekko.stream.scaladsl._
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands._
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.event._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.Transaction
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
import com.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.daml.lf.data.Ref._
import com.daml.lf.data.ScalazEqual._
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.trigger.Runner.{
  SeenMsgs,
  TriggerContext,
  TriggerContextualFlow,
  TriggerContextualSource,
  isHeartbeat,
  isSubmissionFailure,
  isSubmissionSuccess,
  isTransaction,
  logger,
  numberOfActiveContracts,
  numberOfArchiveEvents,
  numberOfCreateEvents,
  numberOfInFlightCommands,
  numberOfPendingContracts,
  triggerUserState,
}
import com.daml.lf.engine.trigger.Runner.Implicits._
import com.daml.lf.engine.trigger.ToLoggingContext._
import com.daml.lf.engine.trigger.UnfoldState.{UnfoldStateShape, flatMapConcatNode, toSource}
import com.daml.lf.language.Ast._
import com.daml.lf.language.PackageInterface
import com.daml.lf.language.Util._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SValue.SMap.`SMap Ordering`
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
import io.grpc.StatusRuntimeException
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.option._
import scalaz.syntax.tag._
import scalaz.{-\/, \/, \/-}

import java.time.{Clock, Instant}
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.{nowarn, tailrec}
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.TreeMap
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
) {

  def initialStateArguments(
      parties: TriggerParties,
      acs: Seq[CreatedEvent],
      triggerConfig: TriggerRunnerConfig,
      converter: Converter,
  ): Array[SValue] = {
    if (defn.version >= Trigger.Version.`2.5.1`) {
      Array(converter.fromTriggerSetupArguments(parties, acs, triggerConfig).orConverterException)
    } else {
      val createdValue: SValue = converter.fromActiveContracts(acs).orConverterException
      val partyArg = SParty(parties.actAs)

      if (hasReadAs) {
        // trigger version SDK 1.18 and newer
        val readAsArg = SList(parties.readAs.map(SParty).to(FrontStack))
        Array(partyArg, readAsArg, createdValue)
      } else {
        // trigger version prior to SDK 1.18
        Array(partyArg, createdValue)
      }
    }
  }
}

/** Throwing an instance of this exception will cause the trigger service to stop the runner
  */
private sealed abstract class TriggerHardLimitException(message: String) extends Exception(message)

private final case class InFlightCommandOverflowException(inFlightCommands: Int, overflowCount: Int)
    extends TriggerHardLimitException(
      s"$inFlightCommands in-flight commands exceed limit of $overflowCount"
    )

private final case class ACSOverflowException(activeContracts: Int, overflowCount: Long)
    extends TriggerHardLimitException(
      s"$activeContracts active contracts exceed limit of $overflowCount"
    )

private final case class TriggerRuleEvaluationTimeout(
    ruleTimeout: FiniteDuration
) extends TriggerHardLimitException(
      s"trigger rule evaluation duration exceeded the limit of $ruleTimeout"
    )

private final case class TriggerRuleStepInterpretationTimeout(
    stepTimeout: FiniteDuration
) extends TriggerHardLimitException(
      s"trigger step interpretation duration exceeded the limit of $stepTimeout"
    )

// Utilities for interacting with the speedy machine.
private[lf] object Machine {

  // Run speedy until we arrive at a value.
  def stepToValue(
      compiledPackages: CompiledPackages,
      expr: SExpr,
  )(implicit triggerContext: TriggerLogContext): SValue = {
    Speedy.Machine.runPureSExpr(expr, compiledPackages) match {
      case Right(v) => v
      case Left(err) =>
        triggerContext.logError(Pretty.prettyError(err).render(80))
        throw err
    }
  }
}

object Trigger {

  private[trigger] def newTriggerLogContext[P, T](
      triggerDefinition: Identifier,
      actAs: Ref.Party,
      readAs: Set[Ref.Party],
      triggerId: String,
      applicationId: Option[Ref.ApplicationId],
  )(f: TriggerLogContext => T): T = {
    LoggingContextOf.newLoggingContext(
      LoggingContextOf.label[Trigger with P],
      "applicationId" -> applicationId.getOrElse[String](""),
    ) { implicit loggingContext: LoggingContextOf[Trigger] =>
      TriggerLogContext.newRootSpan(
        "setup",
        "id" -> triggerId,
        "definition" -> triggerDefinition,
        "actAs" -> actAs,
        "readAs" -> readAs,
      ) { implicit triggerContext: TriggerLogContext =>
        f(triggerContext)
      }
    }
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

    def fromV20ExtractTriggerDefinition(
        ty: TTyCon,
        tyArg: Type,
        triggerIds: TriggerIds,
        version: Trigger.Version,
    ): Either[String, TriggerDefinition] = {
      val tyCon = ty.tycon

      if (tyCon == triggerIds.damlTriggerLowLevel("Trigger")) {
        val expr = EVal(triggerId)
        val ty = TypeConApp(tyCon, ImmArray(tyArg))

        Right(TriggerDefinition(triggerId, ty, version, Level.Low, expr))
      } else if (tyCon == triggerIds.damlTrigger("Trigger")) {
        val runTrigger = EVal(triggerIds.damlTrigger("runTrigger"))
        val expr = EApp(runTrigger, EVal(triggerId))
        val triggerState = TTyCon(triggerIds.damlTriggerInternal("TriggerState"))
        val stateTy = TApp(triggerState, tyArg)
        val lowLevelTriggerTy = triggerIds.damlTriggerLowLevel("Trigger")
        val ty = TypeConApp(lowLevelTriggerTy, ImmArray(stateTy))

        Right(TriggerDefinition(triggerId, ty, version, Level.High, expr))
      } else {
        error(triggerId, ty)
      }
    }

    def fromV251ExtractTriggerDefinition(
        ty: TTyCon,
        tyArg: Type,
        triggerIds: TriggerIds,
        version: Trigger.Version,
    ): Either[String, TriggerDefinition] = {
      val tyCon = ty.tycon

      if (tyCon == triggerIds.damlTriggerLowLevel("BatchTrigger")) {
        val expr = EVal(triggerId)
        val ty = TypeConApp(tyCon, ImmArray(tyArg))

        Right(TriggerDefinition(triggerId, ty, version, Level.Low, expr))
      } else if (tyCon == triggerIds.damlTriggerLowLevel("Trigger")) {
        val runTrigger = EVal(triggerIds.damlTriggerInternal("runLegacyTrigger"))
        val expr = EApp(runTrigger, EVal(triggerId))
        val triggerState = TTyCon(triggerIds.damlTriggerInternal("TriggerState"))
        val stateTy = TApp(triggerState, tyArg)
        val lowLevelTriggerTy = triggerIds.damlTriggerLowLevel("Trigger")
        val ty = TypeConApp(lowLevelTriggerTy, ImmArray(stateTy))

        Right(TriggerDefinition(triggerId, ty, version, Level.Low, expr))
      } else if (tyCon == triggerIds.damlTrigger("Trigger")) {
        val runTrigger = EVal(triggerIds.damlTrigger("runTrigger"))
        val expr = EApp(runTrigger, EVal(triggerId))
        val triggerState = TTyCon(triggerIds.damlTriggerInternal("TriggerState"))
        val stateTy = TApp(triggerState, tyArg)
        val lowLevelTriggerTy = triggerIds.damlTriggerLowLevel("Trigger")
        val ty = TypeConApp(lowLevelTriggerTy, ImmArray(stateTy))

        Right(TriggerDefinition(triggerId, ty, version, Level.High, expr))
      } else {
        error(triggerId, ty)
      }
    }

    pkgInterface.lookupValue(triggerId) match {
      case Right(DValueSignature(TApp(ty @ TTyCon(tyCon), tyArg), _, _)) =>
        val triggerIds = TriggerIds(tyCon.packageId)
        // Ensure version related trigger definition updates take into account the conflateMsgValue code
        detectVersion(pkgInterface, triggerIds).flatMap { version =>
          if (version < Trigger.Version.`2.5.1`) {
            fromV20ExtractTriggerDefinition(ty, tyArg, triggerIds, version)
          } else {
            fromV251ExtractTriggerDefinition(ty, tyArg, triggerIds, version)
          }
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
  )(implicit triggerContext: TriggerLogContext): Either[String, Trigger] = {
    val compiler = compiledPackages.compiler

    for {
      triggerDef <- detectTriggerDefinition(compiledPackages.pkgInterface, triggerId)
      hasReadAs <- detectHasReadAs(compiledPackages.pkgInterface, triggerDef.triggerIds)
      filter <- getTriggerFilter(compiledPackages, compiler, triggerDef)
      heartbeat <- getTriggerHeartbeat(compiledPackages, compiler, triggerDef)
    } yield Trigger(triggerDef, filter, heartbeat, hasReadAs)
  }

  // Return the heartbeat specified by the user.
  private def getTriggerHeartbeat(
      compiledPackages: CompiledPackages,
      compiler: Compiler,
      triggerDef: TriggerDefinition,
  )(implicit triggerContext: TriggerLogContext): Either[String, Option[FiniteDuration]] = {
    val heartbeat = compiler.unsafeCompile(
      ERecProj(triggerDef.ty, Name.assertFromString("heartbeat"), triggerDef.expr)
    )
    Machine.stepToValue(compiledPackages, heartbeat) match {
      case SOptional(None) => Right(None)
      case SOptional(Some(relTime)) => Converter.toFiniteDuration(relTime).map(Some(_))
      case value => Left(s"Expected Optional but got $value.")
    }
  }

  // Return the trigger filter specified by the user.
  def getTriggerFilter(
      compiledPackages: CompiledPackages,
      compiler: Compiler,
      triggerDef: TriggerDefinition,
  )(implicit triggerContext: TriggerLogContext): Either[String, Filters] = {
    val registeredTemplates = compiler.unsafeCompile(
      ERecProj(triggerDef.ty, Name.assertFromString("registeredTemplates"), triggerDef.expr)
    )
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
                includeCreatedEventBlob = false,
              )
          }
        })
      })

    Machine.stepToValue(compiledPackages, registeredTemplates) match {
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
        Converter.toRegisteredTemplates(v) match {
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

  final class Version(protected val rank: Int) extends Ordered[Version] {
    override def compare(that: Version): Int = this.rank compare that.rank

    override def toString: String = s"Version($rank)"
  }
  object Version {
    val `2.0.0` = new Version(0)
    val `2.5.0` = new Version(50)
    val `2.5.1` = new Version(51)
    val `2.6.0` = new Version(60)

    def fromString(s: Option[String]): Either[String, Version] =
      s match {
        case None => Right(`2.0.0`)
        case Some("Version_2_5") => Right(`2.5.0`)
        case Some("Version_2_5_1") => Right(`2.5.1`)
        case Some("Version_2_6") => Right(`2.6.0`)
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
    private[trigger] val compiledPackages: CompiledPackages,
    trigger: Trigger,
    triggerConfig: TriggerRunnerConfig,
    client: LedgerClient,
    timeProviderType: TimeProviderType,
    applicationId: Option[Ref.ApplicationId],
    parties: TriggerParties,
)(implicit triggerContext: TriggerLogContext) {

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
        triggerContext: TriggerLogContext
    ): Unit = {
      val inFlightState = pendingIds.put(uuid, seeOne)

      (inFlightState, seeOne) match {
        case (None, SeenMsgs.Neither) =>
          triggerContext.logDebug("New in-flight command")
          discard(inFlight.incrementAndGet())

        case (None, _) | (Some(SeenMsgs.Neither), SeenMsgs.Neither) =>
        // no work required

        case (Some(SeenMsgs.Neither), _) =>
          triggerContext.logDebug("In-flight command completed")
          discard(inFlight.decrementAndGet())

        case (Some(_), SeenMsgs.Neither) =>
          triggerContext.logDebug("New in-flight command")
          discard(inFlight.incrementAndGet())

        case (Some(_), _) =>
        // no work required
      }
    }

    def remove(uuid: UUID)(implicit triggerContext: TriggerLogContext): Unit = {
      val inFlightState = pendingIds.remove(uuid)

      if (inFlightState.contains(SeenMsgs.Neither)) {
        triggerContext.logDebug("In-flight command completed")
        discard(inFlight.decrementAndGet())
      }
    }
  }

  private[this] val inFlightCommands = new InFlightCommands

  private val transactionFilter =
    TransactionFilter(parties.readers.map(p => (p, trigger.filters)).toMap)

  // return whether uuid *was* present in pendingCommandIds
  private[this] def useCommandId(uuid: UUID, seeOne: SeenMsgs.One)(implicit
      triggerContext: TriggerLogContext
  ): Boolean = {
    triggerContext.enrichTriggerContext(
      "commandId" -> uuid
    ) { implicit triggerContext: TriggerLogContext =>
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
  )(implicit triggerContext: TriggerLogContext): (UUID, SubmitRequest) = {
    val commandUUID = UUID.randomUUID

    triggerContext.enrichTriggerContext(
      "commandId" -> commandUUID,
      "commands" -> commands,
    ) { implicit triggerContext: TriggerLogContext =>
      inFlightCommands.update(commandUUID, SeenMsgs.Neither)

      val commandsArg = Commands(
        ledgerId = client.ledgerId.unwrap,
        applicationId = applicationId.getOrElse(""),
        commandId = commandUUID.toString,
        party = parties.actAs,
        readAs = parties.readAs.toList,
        commands = commands,
      )

      (commandUUID, SubmitRequest(commands = Some(commandsArg)))
    }
  }

  private def freeTriggerSubmits(
      clientTime: Timestamp,
      v: SValue,
      hardLimitKillSwitch: KillSwitch,
  )(implicit
      materializer: Materializer,
      triggerContext: TriggerLogContext,
  ): UnfoldState[SValue, TriggerContext[SubmitRequest]] = {
    var numberOfRuleEvaluations = 0
    var numberOfSubmissions = 0
    var numberOfGetTimes = 0
    var numberOfCreates = 0
    var numberOfExercises = 0
    var numberOfCreateAndExercise = 0
    var numberOfExerciseByKey = 0
    var totalStepIteratorTime = 0L

    val startTime = System.nanoTime()
    val ruleEvaluationTimer = if (triggerConfig.hardLimit.allowTriggerTimeouts) {
      materializer.scheduleOnce(
        triggerConfig.hardLimit.ruleEvaluationTimeout,
        () => {
          triggerContext.logError(
            "Stopping trigger as the rule evaluator has exceeded its allotted running time",
            "rule-evaluation-timeout" -> triggerConfig.hardLimit.ruleEvaluationTimeout,
          )

          hardLimitKillSwitch.abort(
            TriggerRuleEvaluationTimeout(triggerConfig.hardLimit.ruleEvaluationTimeout)
          )
        },
      )
    } else {
      Cancellable.alreadyCancelled
    }

    triggerContext.childSpan("step") { implicit triggerContext: TriggerLogContext =>
      type Termination = SValue \/ (TriggerContext[SubmitRequest], SValue)
      @tailrec def go(v: SValue): Termination = {
        numberOfRuleEvaluations += 1

        val startStepTime = System.nanoTime()
        val stepIteratorTimer = if (triggerConfig.hardLimit.allowTriggerTimeouts) {
          materializer.scheduleOnce(
            triggerConfig.hardLimit.stepInterpreterTimeout,
            () => {
              triggerContext.logError(
                "Stopping trigger as the rule step interpreter has exceeded its allotted running time",
                "step-interpreter-timeout" -> triggerConfig.hardLimit.stepInterpreterTimeout,
              )

              hardLimitKillSwitch.abort(
                TriggerRuleStepInterpretationTimeout(
                  triggerConfig.hardLimit.stepInterpreterTimeout
                )
              )
            },
          )
        } else {
          Cancellable.alreadyCancelled
        }
        val resumed: Termination Either SValue =
          unrollFree(v) match {
            case Right(Right(vvv @ (variant, vv))) =>
              try {
                // Must be kept in-sync with the DAML code LowLevel#TriggerF
                vvv.match2 {
                  case "GetTime" /*(Time -> a)*/ => { case DamlFun(timeA) =>
                    numberOfGetTimes += 1
                    Right(
                      Machine.stepToValue(compiledPackages, makeAppD(timeA, STimestamp(clientTime)))
                    )
                  }
                  case "Submit" /*([Command], Text -> a)*/ => {
                    case DamlTuple2(sCommands, DamlFun(textA)) =>
                      numberOfSubmissions += 1

                      val commands = Converter.toCommands(sCommands).orConverterException
                      val (commandUUID, submitRequest) = handleCommands(commands)

                      numberOfCreates += commands.count(_.command.isCreate)
                      numberOfExercises += commands.count(_.command.isExercise)
                      numberOfCreateAndExercise += commands.count(_.command.isCreateAndExercise)
                      numberOfExerciseByKey += commands.count(_.command.isExerciseByKey)

                      Left(
                        \/-(
                          (
                            Ctx(triggerContext, submitRequest),
                            Machine.stepToValue(
                              compiledPackages,
                              makeAppD(textA, SText((commandUUID: UUID).toString)),
                            ),
                          )
                        ): Termination
                      )
                  }
                  case _ =>
                    triggerContext.logError("Unrecognised TriggerF step", "variant" -> variant)
                    throw new ConverterException(s"unrecognized TriggerF step $variant")
                }(fallback = throw new ConverterException(s"invalid contents for $variant: $vv"))
              } finally {
                val endStepTime = System.nanoTime()

                discard(stepIteratorTimer.cancel())
                totalStepIteratorTime += endStepTime - startStepTime
              }

            case Right(Left(newState)) =>
              try {
                Left(-\/(newState))
              } finally {
                val endStepTime = System.nanoTime()

                discard(stepIteratorTimer.cancel())
                totalStepIteratorTime += endStepTime - startStepTime
              }

            case Left(e) =>
              throw new ConverterException(e)
          }

        resumed match {
          case Left(newState) => newState
          case Right(suspended) => go(suspended)
        }
      }

      UnfoldState(v)({ state =>
        go(state) match {
          case next @ -\/(_) =>
            try {
              val endTime = System.nanoTime()
              val ruleEvaluationTime = endTime - startTime
              val stepIteratorMean = totalStepIteratorTime / numberOfRuleEvaluations
              val stepIteratorDelayLogEntry = if (numberOfRuleEvaluations > 1) {
                val stepIteratorDelayMean =
                  (ruleEvaluationTime - totalStepIteratorTime) / (numberOfRuleEvaluations - 1)
                // Metrics for mean step iterator delays have greatest meaning if there are multiple iteration steps
                LoggingEntries("step-iterator-delay-mean" -> stepIteratorDelayMean.toHumanReadable)
              } else {
                LoggingEntries.empty
              }

              triggerContext.logInfo(
                "Trigger rule evaluation end",
                "metrics" -> LoggingValue.Nested(
                  LoggingEntries(
                    "steps" -> numberOfRuleEvaluations,
                    "get-time" -> numberOfGetTimes,
                    "submissions" -> LoggingValue.Nested(
                      LoggingEntries(
                        "total" -> numberOfSubmissions,
                        "create" -> numberOfCreates,
                        "exercise" -> numberOfExercises,
                        "createAndExercise" -> numberOfCreateAndExercise,
                        "exerciseByKey" -> numberOfExerciseByKey,
                      )
                    ),
                    "duration" -> LoggingValue.Nested(
                      LoggingEntries(
                        "rule-evaluation" -> ruleEvaluationTime.toHumanReadable,
                        "step-iterator-mean" -> stepIteratorMean.toHumanReadable,
                      ) ++ stepIteratorDelayLogEntry
                    ),
                  )
                ),
              )

              next
            } finally {
              discard(ruleEvaluationTimer.cancel())
            }

          case next =>
            next
        }
      })
    }
  }

  // This function produces a source of trigger messages,
  // with input of failures from command submission
  private def msgSource(
      client: LedgerClient,
      offset: LedgerOffset,
      heartbeat: Option[FiniteDuration],
      parties: TriggerParties,
      filter: TransactionFilter,
  )(implicit
      triggerContext: TriggerLogContext
  ): TriggerContextualFlow[SingleCommandFailure, TriggerMsg, NotUsed] = {
    // A queue for command submission failures.
    val submissionFailureQueue: TriggerContextualFlow[SingleCommandFailure, TriggerMsg, NotUsed] = {
      TriggerContextualFlow[SingleCommandFailure]
        .map { case ctx @ Ctx(context, failure, _) =>
          context.childSpan("failure") { implicit triggerContext: TriggerLogContext =>
            triggerContext.logInfo(
              "Received command submission failure from ledger API client",
              "error" -> failure,
            )

            ctx.copy(context = triggerContext)
          }
        }
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
        .buffer(
          triggerConfig.submissionFailureQueueSize,
          OverflowStrategy.fail.withLogLevel(Logging.WarningLevel),
        )
        .map { case ctx @ Ctx(_, SingleCommandFailure(commandId, s), _) =>
          val completion = TriggerMsg.Completion(
            Completion(
              commandId,
              Some(Status(s.getStatus.getCode.value(), s.getStatus.getDescription)),
            )
          )

          ctx.context.logInfo(
            "Rewrite command submission failure for trigger rules",
            "message" -> completion,
          )

          ctx.copy(value = completion)
        }
    }

    // The transaction source (ledger).
    val transactionSource: TriggerContextualSource[TriggerMsg, NotUsed] = {
      triggerContext.logInfo("Subscribing to ledger API transaction source", "filter" -> filter)
      client.transactionClient
        .getTransactions(offset, None, filter)
        .map { transaction =>
          triggerContext.childSpan("update") { implicit triggerContext: TriggerLogContext =>
            triggerContext.logDebug("Transaction source", "message" -> transaction)

            triggerContext.childSpan("evaluation") { implicit triggerContext: TriggerLogContext =>
              Ctx(triggerContext, TriggerMsg.Transaction(transaction))
            }
          }
        }
    }

    // Command completion source (ledger completion stream)
    val completionSource: TriggerContextualSource[TriggerMsg, NotUsed] = {
      triggerContext.logInfo("Subscribing to ledger API completion source")
      client.commandClient
        // Completions only take actAs into account so no need to include readAs.
        .completionSource(List(parties.actAs), offset)
        .collect { case CompletionElement(completion, _) =>
          triggerContext.childSpan("update") { implicit triggerContext: TriggerLogContext =>
            triggerContext.logDebug("Completion source", "message" -> completion)

            triggerContext.childSpan("evaluation") { implicit triggerContext: TriggerLogContext =>
              Ctx(triggerContext, TriggerMsg.Completion(completion))
            }
          }
        }
    }

    // Heartbeats source (we produce these repetitively on a timer with
    // the given delay interval).
    val heartbeatSource: TriggerContextualSource[TriggerMsg, NotUsed] = heartbeat match {
      case Some(interval) =>
        triggerContext.logInfo("Heartbeat source configured", "heartbeat" -> interval)
        Source
          .tick[TriggerMsg](interval, interval, TriggerMsg.Heartbeat)
          .mapMaterializedValue(_ => NotUsed)
          .map { heartbeat =>
            triggerContext.childSpan("update") { implicit triggerContext: TriggerLogContext =>
              triggerContext.logDebug("Heartbeat source", "message" -> "Heartbeat")

              triggerContext.childSpan("evaluation") { implicit triggerContext: TriggerLogContext =>
                Ctx(triggerContext, heartbeat)
              }
            }
          }

      case None =>
        triggerContext.logInfo("No heartbeat source configured")
        Source.empty[TriggerContext[TriggerMsg]]
    }

    submissionFailureQueue
      .merge(completionSource merge transactionSource merge heartbeatSource)
  }

  private[this] def getTriggerInitialStateLambda(
      acs: Seq[CreatedEvent]
  )(implicit triggerContext: TriggerLogContext): SValue = {
    // Compile the trigger initialState LF function to a speedy expression
    val getInitialState: SExpr =
      compiler.unsafeCompile(
        ERecProj(trigger.defn.ty, Name.assertFromString("initialState"), trigger.defn.expr)
      )
    // Setup an application expression of initialState on the ACS.
    val initialState: SExpr =
      makeApp(
        getInitialState,
        trigger.initialStateArguments(parties, acs, triggerConfig, converter),
      )

    Machine
      .stepToValue(compiledPackages, initialState)
      .expect(
        "TriggerSetup",
        { case DamlAnyModuleRecord("TriggerSetup", fts) => fts }: @nowarn(
          "msg=A repeated case parameter or extracted sequence is not matched by a sequence wildcard"
        ),
      )
      .orConverterException
  }

  private[this] def getTriggerUpdateLambda()(implicit
      triggerContext: TriggerLogContext
  ): SValue = {
    // Compile the trigger Update LF function to a speedy expression
    val update: SExpr =
      compiler.unsafeCompile(
        ERecProj(trigger.defn.ty, Name.assertFromString("update"), trigger.defn.expr)
      )

    Machine.stepToValue(compiledPackages, update)
  }

  private[trigger] def encodeMsgs: TriggerContextualFlow[TriggerMsg, SValue, NotUsed] =
    Flow fromFunction {
      case ctx @ Ctx(_, TriggerMsg.Transaction(transaction), _) =>
        ctx.copy(value = converter.fromTransaction(transaction).orConverterException)

      case ctx @ Ctx(_, TriggerMsg.Completion(completion), _) =>
        ctx.copy(value = converter.fromCompletion(completion).orConverterException)

      case ctx @ Ctx(_, TriggerMsg.Heartbeat, _) =>
        ctx.copy(value = converter.fromHeartbeat)
    }

  private[this] def conflateMsgValue: TriggerContextualFlow[SValue, SValue, NotUsed] = {
    val noop = Flow[TriggerContext[SValue]]

    // Ensure version related trigger definition updates take into account the detectTriggerDefinition code
    if (trigger.defn.version < Trigger.Version.`2.5.1`) {
      noop
    } else {
      noop
        .groupedWithin(triggerConfig.maximumBatchSize.toInt, triggerConfig.batchingDuration)
        // We drop empty message batches only here
        .collect { case batch @ ctx +: remaining =>
          ctx.context.childSpan("batch") { implicit triggerContext: TriggerLogContext =>
            val batchContext = triggerContext.groupWith(remaining.map(_.context): _*)
            val batchValue = batch.map(_.value)
            val total = batchValue.size
            val failures = batchValue.count(isSubmissionFailure)
            val completions = batchValue.count(isSubmissionSuccess)
            val transactions = batchValue.count(isTransaction)
            val creates = batchValue.map(numberOfCreateEvents).sum
            val archives = batchValue.map(numberOfArchiveEvents).sum
            val heartbeats = batchValue.count(isHeartbeat)

            batchContext.logInfo(
              "Trigger rule message batching",
              "metrics" -> LoggingValue.Nested(
                LoggingEntries(
                  "size" -> total,
                  "completions" -> LoggingValue.Nested(
                    LoggingEntries(
                      "failures" -> failures,
                      "successes" -> completions,
                    )
                  ),
                  "transactions" -> LoggingValue.Nested(
                    LoggingEntries(
                      "total" -> transactions,
                      "creates" -> creates,
                      "archives" -> archives,
                    )
                  ),
                  "heartbeats" -> heartbeats,
                )
              ),
            )

            ctx.copy(
              context = batchContext,
              value = SList(FrontStack(batchValue: _*)),
            )
          }
        }
    }
  }

  private[trigger] def runInitialState(clientTime: Timestamp, killSwitch: KillSwitch)(
      acs: Seq[CreatedEvent]
  )(implicit
      materializer: Materializer,
      triggerContext: TriggerLogContext,
  ): Graph[SourceShape2[SValue, TriggerContext[SubmitRequest]], NotUsed] = {
    val startState = getTriggerInitialStateLambda(acs)

    toSource(
      freeTriggerSubmits(clientTime, startState, killSwitch)
        .leftMap { state =>
          triggerContext.logDebug(
            "Trigger rule initial state",
            "state" -> triggerUserState(state, trigger.defn.level, trigger.defn.version),
          )
          if (trigger.defn.level == Trigger.Level.High) {
            triggerContext.logTrace(
              "Trigger rule initial state",
              "state" -> state,
            )
          }
          triggerContext.logInfo(
            "Trigger rule initialization start",
            "metrics" -> LoggingValue.Nested(
              LoggingEntries(
                "acs" -> LoggingValue.Nested(
                  LoggingEntries(
                    "active" -> acs.length,
                    "pending" -> 0,
                  )
                ),
                "in-flight" -> 0,
              )
            ),
          )

          val activeContracts = acs.length
          if (activeContracts > triggerConfig.hardLimit.maximumActiveContracts) {
            triggerContext.logError(
              "Due to an excessive number of active contracts, stopping the trigger",
              "active-contracts" -> activeContracts,
              "active-contract-overflow-count" -> triggerConfig.hardLimit.maximumActiveContracts,
            )
            throw ACSOverflowException(
              activeContracts,
              triggerConfig.hardLimit.maximumActiveContracts,
            )
          }

          triggerContext.logInfo(
            "Trigger rule initialization end",
            "metrics" -> LoggingValue.Nested(
              LoggingEntries(
                "acs" -> LoggingValue.Nested(
                  LoggingEntries(
                    "active" -> numberOfActiveContracts(
                      state,
                      trigger.defn.level,
                      trigger.defn.version,
                    ),
                    "pending" -> numberOfPendingContracts(
                      state,
                      trigger.defn.level,
                      trigger.defn.version,
                    ),
                  )
                ),
                "in-flight" -> numberOfInFlightCommands(
                  state,
                  trigger.defn.level,
                  trigger.defn.version,
                ),
              )
            ),
          )

          state
        }
    )
  }

  private[trigger] def runRuleOnMsgs(
      killSwitch: KillSwitch
  )(implicit
      materializer: Materializer,
      triggerContext: TriggerLogContext,
  ): Graph[
    UnfoldStateShape[SValue, TriggerContext[SValue], TriggerContext[SubmitRequest]],
    NotUsed,
  ] = flatMapConcatNode { (state: SValue, messageVal: TriggerContext[SValue]) =>
    val updateStateLambda = getTriggerUpdateLambda()

    messageVal.context.enrichTriggerContext() { implicit triggerContext: TriggerLogContext =>
      triggerContext.logDebug(
        "Trigger rule evaluation",
        "state" -> triggerUserState(state, trigger.defn.level, trigger.defn.version),
        "message" -> messageVal.value,
      )
      if (trigger.defn.level == Trigger.Level.High) {
        triggerContext.logInfo(
          "Trigger rule evaluation start",
          "metrics" -> LoggingValue.Nested(
            LoggingEntries(
              "in-flight" -> numberOfInFlightCommands(
                state,
                trigger.defn.level,
                trigger.defn.version,
              ),
              "acs" -> LoggingValue.Nested(
                LoggingEntries(
                  "active" -> numberOfActiveContracts(
                    state,
                    trigger.defn.level,
                    trigger.defn.version,
                  ),
                  "pending" -> numberOfPendingContracts(
                    state,
                    trigger.defn.level,
                    trigger.defn.version,
                  ),
                )
              ),
            )
          ),
        )
      }

      val clientTime: Timestamp =
        Timestamp.assertFromInstant(Runner.getCurrentTime(timeProviderType))
      val stateFun = Machine
        .stepToValue(compiledPackages, makeAppD(updateStateLambda, messageVal.value))
        .expect(
          "TriggerRule",
          { case DamlAnyModuleRecord("TriggerRule", DamlAnyModuleRecord("StateT", fun)) =>
            fun
          }: @nowarn("msg=A repeated case parameter .* is not matched by a sequence wildcard"),
        )
        .orConverterException
      val updateWithNewState = Machine.stepToValue(compiledPackages, makeAppD(stateFun, state))

      freeTriggerSubmits(clientTime, v = updateWithNewState, killSwitch)
        .leftMap(
          _.expect(
            "TriggerRule new state",
            { case DamlTuple2(SUnit, newState) =>
              triggerContext.logDebug(
                "Trigger rule state updated",
                "state" -> triggerUserState(state, trigger.defn.level, trigger.defn.version),
              )
              if (trigger.defn.level == Trigger.Level.High) {
                triggerContext.logTrace(
                  "Trigger rule state updated",
                  "state" -> newState,
                )
                triggerContext.logInfo(
                  "Trigger rule evaluation end",
                  "metrics" -> LoggingValue.Nested(
                    LoggingEntries(
                      "in-flight" -> numberOfInFlightCommands(
                        newState,
                        trigger.defn.level,
                        trigger.defn.version,
                      ),
                      "acs" -> LoggingValue.Nested(
                        LoggingEntries(
                          "active" -> numberOfActiveContracts(
                            newState,
                            trigger.defn.level,
                            trigger.defn.version,
                          ),
                          "pending" -> numberOfPendingContracts(
                            newState,
                            trigger.defn.level,
                            trigger.defn.version,
                          ),
                        )
                      ),
                    )
                  ),
                )
              }

              numberOfActiveContracts(newState, trigger.defn.level, trigger.defn.version) match {
                case Some(activeContracts)
                    if activeContracts > triggerConfig.hardLimit.maximumActiveContracts =>
                  triggerContext.logError(
                    "Due to an excessive number of active contracts, stopping the trigger",
                    "active-contracts" -> activeContracts,
                    "active-contract-overflow-count" -> triggerConfig.hardLimit.maximumActiveContracts,
                  )
                  throw ACSOverflowException(
                    activeContracts,
                    triggerConfig.hardLimit.maximumActiveContracts,
                  )

                case _ =>
                  newState
              }
            },
          ).orConverterException
        )
    }
  }

  // A flow for trigger messages representing a process for the
  // accumulated state changes resulting from application of the
  // messages given the starting state represented by the ACS
  // argument.
  private def getTriggerEvaluator(
      acs: Seq[CreatedEvent]
  )(implicit
      materializer: Materializer,
      triggerContext: TriggerLogContext,
  ): TriggerContextualFlow[TriggerMsg, SubmitRequest, Future[SValue]] = {
    triggerContext.logInfo("Trigger starting")

    val clientTime: Timestamp =
      Timestamp.assertFromInstant(Runner.getCurrentTime(timeProviderType))
    val hardLimitKillSwitch = KillSwitches.shared("hard-limit")

    import UnfoldState.{flatMapConcatNodeOps, toSourceOps}

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

      val msgIn = gb add TriggerContextualFlow[TriggerMsg]
      val initialState = gb add runInitialState(clientTime, hardLimitKillSwitch)(acs)
      val initialStateOut = gb add Broadcast[SValue](2)
      val rule = gb add runRuleOnMsgs(hardLimitKillSwitch)
      val submissions = gb add Merge[TriggerContext[SubmitRequest]](2)
      val finalStateIn = gb add Concat[SValue](2)
      val killSwitch = gb add hardLimitKillSwitch.flow[TriggerContext[SValue]]
      // format: off
      initialState.finalState                    ~> initialStateOut                                   ~> rule.initState
      initialState.elemsOut                          ~> submissions
                          msgIn ~> hideIrrelevantMsgs ~> encodeMsgs ~> conflateMsgValue ~> killSwitch ~> rule.elemsIn
                                                        submissions                                   <~ rule.elemsOut
                                                    initialStateOut                                                       ~> finalStateIn
                                                                                                         rule.finalStates ~> finalStateIn ~> saveLastState
      // format: on
      new FlowShape(msgIn.in, submissions.out)
    }

    Flow.fromGraph(graph)
  }

  private[this] def catchIAE[A](a: => A): Option[A] =
    try Some(a)
    catch { case _: IllegalArgumentException => None }

  private[this] def hideIrrelevantMsgs: TriggerContextualFlow[TriggerMsg, TriggerMsg, NotUsed] =
    TriggerContextualFlow[TriggerMsg].mapConcat[TriggerContext[TriggerMsg]] {
      case ctx @ Ctx(_, TriggerMsg.Completion(c), _) =>
        // This happens for invalid UUIDs which we might get for
        // completions not emitted by the trigger.
        val optUuid = catchIAE(UUID.fromString(c.commandId))
        optUuid.fold(List.empty[TriggerContext[TriggerMsg]]) { uuid =>
          if (useCommandId(uuid, SeenMsgs.Completion)(ctx.context)) {
            List(ctx)
          } else {
            List.empty
          }
        }

      case ctx @ Ctx(_, TriggerMsg.Transaction(t), _) =>
        // This happens for invalid UUIDs which we might get for
        // transactions not emitted by the trigger.
        val optUuid = catchIAE(UUID.fromString(t.commandId))
        val hiddenCmd: TriggerContext[TriggerMsg] =
          ctx.copy(value = TriggerMsg.Transaction(t.copy(commandId = "")))
        optUuid.fold(List(hiddenCmd)) { uuid =>
          if (useCommandId(uuid, SeenMsgs.Transaction)(ctx.context)) {
            List(ctx)
          } else {
            List(hiddenCmd)
          }
        }

      case ctx @ Ctx(_, TriggerMsg.Heartbeat, _) =>
        List(ctx) // Heartbeats don't carry any information.
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
  ): TriggerContextualFlow[SubmitRequest, SingleCommandFailure, NotUsed] = {
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

    // Used to defend (by raising an unhandled exception) against trigger rules that emit excessive numbers of command submission requests
    val overflowFlow: TriggerContextualFlow[SubmitRequest, SubmitRequest, NotUsed] =
      TriggerContextualFlow[SubmitRequest]
        .map {
          case Ctx(context, request, _)
              if triggerConfig.hardLimit.allowInFlightCommandOverflows && inFlightCommands.count > triggerConfig.hardLimit.inFlightCommandOverflowCount =>
            context.logError(
              "Due to excessive in-flight commands, stopping the trigger",
              "commandId" -> request.getCommands.commandId,
              "in-flight-commands" -> inFlightCommands.count,
              "in-flight-command-overflow-count" -> triggerConfig.hardLimit.inFlightCommandOverflowCount,
            )

            throw InFlightCommandOverflowException(
              inFlightCommands.count,
              triggerConfig.hardLimit.inFlightCommandOverflowCount,
            )

          case ctx =>
            ctx
        }
    // Used to limit rate (by using back pressure to slow trigger rule evaluation) ledger command submission requests
    val throttleFlow: TriggerContextualFlow[SubmitRequest, SubmitRequest, NotUsed] =
      TriggerContextualFlow[SubmitRequest]
        .throttle(triggerConfig.maxSubmissionRequests, triggerConfig.maxSubmissionDuration)
    // Main ledger command submission flow
    val submissionFlow =
      TriggerContextualFlow[SubmitRequest]
        .via(
          retrying(
            initialTries = triggerConfig.maxRetries,
            backoff = overloadedRetryDelay,
            parallelism = triggerConfig.parallelism,
            retryableSubmit,
            submit,
          )
            .collect { case ctx @ Ctx(context, Some(err), _) =>
              context.logWarning("Ledger API command submission request failed", "error" -> err)

              ctx.copy(value = err)
            }
        )

    overflowFlow.via(throttleFlow).via(submissionFlow)
  }

  // Run the trigger given the state of the ACS. The msgFlow argument
  // passed from ServiceMain is a kill switch. Other choices are
  // possible e.g. 'Flow[TriggerMsg].take()' and this fact is made use
  // of in the test-suite.
  def runWithACS[T](
      acs: Seq[CreatedEvent],
      offset: LedgerOffset,
      msgFlow: Graph[FlowShape[TriggerContext[TriggerMsg], TriggerContext[TriggerMsg]], T] =
        TriggerContextualFlow[TriggerMsg],
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): (T, Future[SValue]) = {
    triggerContext.nextSpan("rule", "ledger-offset" -> offset.toString) {
      implicit triggerContext: TriggerLogContext =>
        val source = msgSource(client, offset, trigger.heartbeat, parties, transactionFilter)
        val triggerRuleEvaluator = triggerContext.childSpan("initialization") {
          implicit triggerContext: TriggerLogContext =>
            getTriggerEvaluator(acs)
        }

        Flow
          .fromGraph(msgFlow)
          .viaMat(triggerRuleEvaluator)(Keep.both)
          .via(submitOrFail)
          .join(source)
          .run()
    }
  }
}

object Runner {

  private[trigger] implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  type TriggerContext[+Value] = Ctx[TriggerLogContext, Value]

  type TriggerContextualSource[+Out, +Mat] = Source[TriggerContext[Out], Mat]

  type TriggerContextualFlow[-In, +Out, +Mat] = Flow[TriggerContext[In], TriggerContext[Out], Mat]

  def TriggerContextualFlow[In]: TriggerContextualFlow[In, In, NotUsed] =
    Flow.apply[TriggerContext[In]]

  def apply(
      compiledPackages: CompiledPackages,
      trigger: Trigger,
      triggerConfig: TriggerRunnerConfig,
      client: LedgerClient,
      timeProviderType: TimeProviderType,
      applicationId: Option[Ref.ApplicationId],
      parties: TriggerParties,
  )(implicit triggerContext: TriggerLogContext): Runner = {
    triggerContext.enrichTriggerContext(
      "level" -> trigger.defn.level.toString,
      "version" -> trigger.defn.version.toString,
    ) { implicit triggerContext: TriggerLogContext =>
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

  private def mapSize(smap: SValue): Int = {
    smap.expect("SMap", { case SMap(_, values) => values.size }).orConverterException
  }

  private def mapLookup(key: SValue, smap: SValue): Either[String, SValue] = {
    smap.expect("SMap", { case SMap(_, entries) if entries.contains(key) => entries(key) })
  }

  private[trigger] def getActiveContracts(
      svalue: SValue,
      level: Trigger.Level,
      version: Trigger.Version,
  ): Option[TreeMap[SValue, TreeMap[SValue, SValue]]] = {
    level match {
      case Trigger.Level.High if version <= Trigger.Version.`2.0.0` =>
        // For older trigger code, we do not support extracting active contracts from the ACS
        None

      case Trigger.Level.High =>
        // The following code should be kept in sync with the ACS variant type in Internal.daml
        // svalue: TriggerState s
        val result = for {
          acs <- svalue.expect("SRecord", { case SRecord(_, _, values) => values.get(0) })
          activeContracts <- acs.expect("SRecord", { case SRecord(_, _, values) => values.get(0) })
          templateMap <- activeContracts.expect(
            "SMap",
            { case SMap(_, values) => values },
          )
          contractMap = templateMap.map { case (templateId, smap) =>
            smap.expect("SMap", { case SMap(_, values) => (templateId, values) })
          }
          resultMap <- contractMap
            .foldRight[Either[String, TreeMap[SValue, TreeMap[SValue, SValue]]]](
              Right(TreeMap.empty)
            ) { case (value, result) =>
              for {
                entry <- value
                res <- result
                (tid, tmap) = entry
              } yield res + (tid -> tmap)
            }
        } yield resultMap

        Some(result.orConverterException)

      case Trigger.Level.Low =>
        None
    }
  }

  private[trigger] def numberOfActiveContracts(
      svalue: SValue,
      level: Trigger.Level,
      version: Trigger.Version,
  ): Option[Int] = {
    getActiveContracts(svalue, level, version).map(_.values.map(_.values.size).sum)
  }

  private[trigger] def numberOfPendingContracts(
      svalue: SValue,
      level: Trigger.Level,
      version: Trigger.Version,
  ): Option[Int] = {
    level match {
      case Trigger.Level.High if version <= Trigger.Version.`2.0.0` =>
        // For older trigger code, we do not support extracting pending contracts from the ACS
        None

      case Trigger.Level.High =>
        // The following code should be kept in sync with the ACS variant type in Internal.daml
        // svalue: TriggerState s
        val result = for {
          acs <- svalue.expect("SRecord", { case SRecord(_, _, values) => values.get(0) })
          pendingContracts <- acs.expect(
            "SRecord",
            { case SRecord(_, _, values) if values.size() >= 1 => values.get(1) },
          )
          size <- pendingContracts.expect(
            "SMap",
            { case SMap(_, values) => values.size },
          )
        } yield size

        Some(result.orConverterException)

      case Trigger.Level.Low =>
        None
    }
  }

  private[trigger] def getInFlightCommands(svalue: SValue): Either[String, SValue] = {
    // The following code should be kept in sync with the TriggerState record type in Internal.daml
    // svalue: TriggerState s
    for {
      inFlightCommands <- svalue.expect(
        "SRecord",
        { case SRecord(_, _, values) if values.size() >= 4 => values.get(4) },
      )
    } yield inFlightCommands
  }

  private[trigger] def numberOfInFlightCommands(
      svalue: SValue,
      level: Trigger.Level,
      version: Trigger.Version,
  ): Option[Int] = {
    // svalue: TriggerState s
    level match {
      case Trigger.Level.High if version <= Trigger.Version.`2.0.0` =>
        // For older trigger code, we do not support extracting commands that are in-flight
        None

      case Trigger.Level.High =>
        val result = for {
          inFlightCommands <- getInFlightCommands(svalue)
        } yield mapSize(inFlightCommands)

        Some(result.orConverterException)

      case Trigger.Level.Low =>
        None
    }
  }

  private def isCreateCommand(svalue: SValue): Boolean = {
    svalue
      .expect(
        "SVariant",
        {
          case SVariant(_, "CreateCommand", _, _) => true
          case SVariant(_, _, _, _) => false
        },
      )
      .orConverterException
  }

  private def isCreateAndExerciseCommand(svalue: SValue): Boolean = {
    svalue
      .expect(
        "SVariant",
        {
          case SVariant(_, "CreateAndExerciseCommand", _, _) => true
          case SVariant(_, _, _, _) => false
        },
      )
      .orConverterException
  }

  private[trigger] def numberOfInFlightCreateCommands(
      commandId: SValue,
      svalue: SValue,
      level: Trigger.Level,
      version: Trigger.Version,
  ): Option[Int] = {
    // svalue: TriggerState s
    level match {
      case Trigger.Level.High if version <= Trigger.Version.`2.0.0` =>
        // For older trigger code, we do not support extracting commands that are in-flight
        None

      case Trigger.Level.High =>
        val result = for {
          inFlightCommands <- getInFlightCommands(svalue)
          commands <- mapLookup(commandId, inFlightCommands)
          entries <- commands.expect("SList", { case SList(entries) => entries })
        } yield entries.toImmArray.toSeq.count { cmd =>
          isCreateCommand(cmd) || isCreateAndExerciseCommand(cmd)
        }

        Some(result.getOrElse(0))

      case Trigger.Level.Low =>
        None
    }
  }

  private def triggerUserState(
      state: SValue,
      level: Trigger.Level,
      version: Trigger.Version,
  ): SValue = {
    // state: TriggerState s
    level match {
      case Trigger.Level.High if version <= Trigger.Version.`2.0.0` =>
        // For old trigger code, we do not support extracting the user state
        state

      case Trigger.Level.High =>
        state
          .expect(
            "SRecord",
            { case SRecord(_, _, values) =>
              values.get(3)
            },
          )
          .orConverterException

      case Trigger.Level.Low =>
        state
    }
  }

  private def isSubmissionFailure(svalue: SValue): Boolean = {
    // svalue: Message
    val result = for {
      values <- svalue.expect(
        "SVariant",
        {
          case SVariant(_, "MCompletion", _, SRecord(_, _, values)) if values.size() >= 1 =>
            values.get(1)
        },
      )
      _ <- values.expect("SVariant", { case SVariant(_, "Failed", _, _) => true })
    } yield ()

    result.isRight
  }

  private def isSubmissionSuccess(svalue: SValue): Boolean = {
    // svalue: Message
    val result = for {
      values <- svalue.expect(
        "SVariant",
        {
          case SVariant(_, "MCompletion", _, SRecord(_, _, values)) if values.size() >= 1 =>
            values.get(1)
        },
      )
      _ <- values.expect("SVariant", { case SVariant(_, "Succeeded", _, _) => true })
    } yield ()

    result.isRight
  }

  private def isMessage(svalue: SValue): Boolean = {
    // svalue: Message
    isTransaction(svalue) || isCompletion(svalue) || isHeartbeat(svalue)
  }

  private def isCompletion(svalue: SValue): Boolean = {
    // svalue: Message
    svalue.expect("SVariant", { case SVariant(_, "MCompletion", _, _) => true }).isRight
  }

  private def isTransaction(svalue: SValue): Boolean = {
    // svalue: Message
    svalue.expect("SVariant", { case SVariant(_, "MTransaction", _, _) => true }).isRight
  }

  private def isCreateEvent(svalue: SValue): Boolean = {
    // svalue: Event
    svalue.expect("SVariant", { case SVariant(_, "CreatedEvent", _, _) => true }).isRight
  }

  private def numberOfCreateEvents(svalue: SValue): Int = {
    // svalue: Message
    val result = for {
      values <- svalue.expect(
        "SVariant",
        {
          case SVariant(_, "MTransaction", _, SRecord(_, _, values)) if values.size() >= 2 =>
            values.get(2)
        },
      )
      events <- values.expect("SList", { case SList(events) => events.toImmArray })
    } yield events.filter(isCreateEvent).length

    if (isMessage(svalue)) {
      result.getOrElse(0)
    } else {
      result.orConverterException
    }
  }

  private def isArchiveEvent(svalue: SValue): Boolean = {
    // svalue: Event
    svalue.expect("SVariant", { case SVariant(_, "ArchivedEvent", _, _) => true }).isRight
  }

  private def numberOfArchiveEvents(svalue: SValue): Int = {
    // svalue: Message
    val result = for {
      values <- svalue.expect(
        "SVariant",
        {
          case SVariant(_, "MTransaction", _, SRecord(_, _, values)) if values.size() >= 2 =>
            values.get(2)
        },
      )
      events <- values.expect("SList", { case SList(events) => events.toImmArray })
    } yield events.filter(isArchiveEvent).length

    if (isMessage(svalue)) {
      result.getOrElse(0)
    } else {
      result.orConverterException
    }
  }

  private def isHeartbeat(svalue: SValue): Boolean = {
    // svalue: Message
    svalue
      .expect(
        "SVariant",
        { case SVariant(_, "MHeartbeat", _, _) =>
          true
        },
      )
      .isRight
  }

  private def overloadedRetryDelay(afterTries: Int): FiniteDuration =
    (250 * (1 << (afterTries - 1))).milliseconds

  // Return the time provider for a given time provider type.
  def getCurrentTime(ty: TimeProviderType): Instant = {
    ty match {
      case TimeProviderType.Static => Instant.EPOCH
      case TimeProviderType.WallClock => Clock.systemUTC().instant()
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
      value.context.enrichTriggerContext(
        "max-retries" -> initialTries
      ) { implicit triggerContext: TriggerLogContext =>
        if (tries <= 1) {
          notRetryable(value).recoverWith { case error: Throwable =>
            triggerContext.logError(
              "Failing permanently after exhausting submission retries",
              "attempt" -> initialTries,
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

                triggerContext.logWarning(
                  "Submission failed, will retry again after backoff period expires",
                  "attempt" -> attempt,
                  "backoff-period" -> backoffPeriod,
                )

                Thread.sleep(backoffPeriod.toMillis)
              }.flatMap(_ => trial(tries - 1, value)),
            )
          )
        }
      }
    }

    Flow[TriggerContext[A]].mapAsync(parallelism) { ctx =>
      ctx.context.childSpan("submission") { implicit triggerContext: TriggerLogContext =>
        triggerContext.logDebug("Submitting request to ledger API")

        trial(initialTries, ctx.copy(context = triggerContext)).map(b => ctx.copy(value = b))
      }
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
      applicationId: Option[Ref.ApplicationId],
      parties: TriggerParties,
      config: Compiler.Config,
      triggerConfig: TriggerRunnerConfig,
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[SValue] =
    Trigger.newTriggerLogContext(
      triggerId,
      parties.actAs,
      parties.readAs,
      triggerId.toString,
      applicationId,
    ) { implicit triggerContext: TriggerLogContext =>
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
    implicit class DurationExtensions(duration: Long) {
      def toHumanReadable: String = {
        if (duration >= 24L * 60L * 60L * 1000L * 1000L * 1000L) {
          s"${duration / (24L * 60L * 60L * 1000L * 1000L * 1000L)}days"
        } else if (duration >= 60L * 60L * 1000L * 1000L * 1000L) {
          s"${duration / (60L * 60L * 1000L * 1000L * 1000L)}hrs"
        } else if (duration >= 60L * 1000L * 1000L * 1000L) {
          s"${duration / (60L * 1000L * 1000L * 1000L)}mins"
        } else if (duration >= 1000L * 1000L * 1000L) {
          s"${duration / (1000L * 1000L * 1000L)}s"
        } else if (duration >= 1000L * 1000L) {
          s"${duration / (1000L * 1000L)}ms"
        } else if (duration >= 1000L) {
          s"${duration / 1000L}us"
        } else {
          s"${duration}ns"
        }
      }
    }

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

    implicit def `VariantConName to LoggingValue`: ToLoggingValue[VariantConName] =
      ToStringToLoggingValue

    implicit def `Status to LoggingValue`: ToLoggingValue[Status] = status =>
      LoggingValue.Nested(
        LoggingEntries(
          "message" -> status.message,
          "code" -> status.code,
        )
      )

    implicit def `CompletionMsg to LoggingValue`: ToLoggingValue[TriggerMsg.Completion] =
      completion =>
        LoggingValue.Nested(
          LoggingEntries(
            "commandId" -> completion.c.commandId,
            "status" -> completion.c.getStatus.code,
            "message" -> completion.c.getStatus.message,
          )
        )

    implicit def `SingleCommandFailure to LoggingValue`: ToLoggingValue[SingleCommandFailure] =
      failure =>
        LoggingValue.Nested(
          LoggingEntries(
            "commandId" -> failure.commandId,
            "status" -> failure.s.getStatus.getCode.value(),
            "statusName" -> failure.s.getStatus.getCode.name(),
            "message" -> failure.s.getStatus.getDescription,
          )
        )

    implicit def `api.Value to LoggingValue`: ToLoggingValue[api.Value] = value =>
      if (logger.withoutContext.isTraceEnabled) {
        PrettyPrint.prettyApiValue(verbose = true)(value).render(80)
      } else {
        PrettyPrint.prettyApiValue(verbose = true, maxListWidth = Some(20))(value).render(80)
      }

    implicit def `SValue to LoggingValue`: ToLoggingValue[SValue] = value =>
      PrettyPrint.prettySValue(value).render(80)

    // Allows using deprecated Protobuf fields
    @annotation.nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\..*")
    implicit def `TransactionFilter to LoggingValue`: ToLoggingValue[TransactionFilter] = filter =>
      LoggingValue.Nested(LoggingEntries(filter.filtersByParty.view.mapValues { value =>
        LoggingValue.Nested(
          LoggingEntries(
            "templateIds" -> value.getInclusive.templateIds,
            "interfaceIds" -> value.getInclusive.interfaceFilters.map(_.getInterfaceId),
          )
        )
      }.toSeq: _*))

    implicit def `CreatedEvent to LoggingEvent`: ToLoggingValue[CreatedEvent] = created =>
      LoggingValue.Nested(
        LoggingEntries(
          "type" -> "CreatedEvent",
          "id" -> created.eventId,
          "contractId" -> created.contractId,
          "templateId" -> created.getTemplateId,
          "interfaceViews" -> LoggingValue.OfIterable(
            created.interfaceViews.map(view =>
              LoggingValue.Nested(
                LoggingEntries(
                  "interfaceId" -> view.getInterfaceId,
                  "viewStatus" -> view.getViewStatus,
                )
              )
            )
          ),
          "witnessParties" -> created.witnessParties,
          "signatories" -> created.signatories,
          "observers" -> created.observers,
        ) ++ created.contractKey.fold(LoggingEntries.empty) { contractKey =>
          LoggingEntries(
            "contractKey" -> contractKey
          )
        }
      )

    implicit def `ArchivedEvent to LoggingValue`: ToLoggingValue[ArchivedEvent] = archived =>
      LoggingValue.Nested(
        LoggingEntries(
          "type" -> "ArchivedEvent",
          "id" -> archived.eventId,
          "contractId" -> archived.contractId,
          "templateId" -> archived.getTemplateId,
          "witnessParties" -> archived.witnessParties,
        )
      )

    implicit def `Transaction to LoggingValue`: ToLoggingValue[Transaction] = transaction =>
      LoggingValue.Nested(
        LoggingEntries(
          "transactionId" -> transaction.transactionId,
          "commandId" -> transaction.commandId,
          "workflowId" -> transaction.workflowId,
          "offset" -> transaction.offset,
          "events" -> LoggingValue.OfIterable(transaction.events.collect {
            case Event(Event.Event.Created(created)) =>
              created

            case Event(Event.Event.Archived(archived)) =>
              archived
          }),
        )
      )

    implicit def `Completion to LoggingValue`: ToLoggingValue[Completion] = completion =>
      LoggingValue.Nested(
        LoggingEntries(
          "commandId" -> completion.commandId,
          "status" -> completion.getStatus.code,
          "message" -> completion.getStatus.message,
        )
      )

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
