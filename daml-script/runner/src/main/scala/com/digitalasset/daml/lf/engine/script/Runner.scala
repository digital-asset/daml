// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ParticipantsJsonProtocol.ContractIdFormat
import com.daml.lf.engine.script.ledgerinteraction.{
  GrpcLedgerClient,
  JsonLedgerClient,
  ScriptLedgerClient,
  ScriptTimeMode,
}
import com.daml.lf.typesig.EnvironmentSignature
import com.daml.lf.typesig.reader.SignatureReader
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LanguageVersion, PackageInterface}
import com.daml.lf.interpretation.{Error => IE}
import com.daml.lf.speedy.SBuiltin.SBToAny
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{
  ArrayList,
  Compiler,
  Pretty,
  SDefinition,
  SError,
  SValue,
  Speedy,
  TraceLog,
  WarningLog,
}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.logging.LoggingContext
import com.daml.script.converter.Converter.unrollFree
import com.daml.script.converter.ConverterException
import com.typesafe.scalalogging.StrictLogging
import scalaz.OneAnd._
import scalaz.std.either._
import scalaz.std.map._
import scalaz.std.option._
import scalaz.std.scalaFuture._
import scalaz.std.set._
import scalaz.syntax.traverse._
import scalaz.{Applicative, NonEmptyList, OneAnd, Traverse, \/-}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object LfValueCodec extends ApiCodecCompressed(false, false)

case class Participant(participant: String)
case class ApiParameters(
    host: String,
    port: Int,
    access_token: Option[String],
    application_id: Option[ApplicationId],
)
case class Participants[+T](
    default_participant: Option[T],
    participants: Map[Participant, T],
    party_participants: Map[Party, Participant],
) {
  private def getPartyParticipant(party: Party): Either[String, T] =
    party_participants.get(party) match {
      case None =>
        default_participant.toRight(s"No participant for party $party and no default participant")
      case Some(participant) => getParticipant(Some(participant))
    }
  def getPartiesParticipant(parties: OneAnd[Set, Party]): Either[String, T] = {
    import scalaz.syntax.foldable._
    for {
      participants <- NonEmptyList[Party](parties.head, parties.tail.toList: _*)
        .traverse(getPartyParticipant(_))
      participant <-
        if (participants.all(_ == participants.head)) {
          Right(participants.head)
        } else {
          Left(
            s"All parties must be on the same participant but parties were allocated as follows: ${parties.toList
                .zip(participants.toList)}"
          )
        }
    } yield participant
  }

  def getParticipant(participantOpt: Option[Participant]): Either[String, T] =
    participantOpt match {
      case None =>
        default_participant.toRight(s"No default participant")
      case Some(participant) =>
        participants
          .get(participant)
          .orElse(default_participant)
          .toRight(s"No participant $participant and no default participant")
    }
}

object Participants {
  implicit val darTraverse: Traverse[Participants] = new Traverse[Participants] {
    override def map[A, B](fa: Participants[A])(f: A => B): Participants[B] =
      Participants[B](
        fa.default_participant.map(f),
        fa.participants.map({ case (k, v) =>
          (k, f(v))
        }),
        fa.party_participants,
      )

    override def traverseImpl[G[_]: Applicative, A, B](
        fa: Participants[A]
    )(f: A => G[B]): G[Participants[B]] = {
      import scalaz.syntax.apply._
      import scalaz.syntax.traverse._
      val gb: G[Option[B]] = fa.default_participant.traverse(f)
      val gbs: G[Map[Participant, B]] = fa.participants.traverse(f)
      ^(gb, gbs)((b, bs) => Participants(b, bs, fa.party_participants))
    }
  }
}

object ParticipantsJsonProtocol extends DefaultJsonProtocol {
  implicit object ParticipantFormat extends JsonFormat[Participant] {
    def read(value: JsValue) = value match {
      case JsString(s) => Participant(s)
      case _ => deserializationError("Expected Participant string")
    }
    def write(p: Participant) = JsString(p.participant)
  }
  implicit object PartyFormat extends JsonFormat[Party] {
    def read(value: JsValue) = value match {
      case JsString(s) => Party.fromString(s).fold(deserializationError(_), identity)
      case _ => deserializationError("Expected Party string")
    }
    def write(p: Party) = JsString(p)
  }
  implicit val ContractIdFormat: JsonFormat[ContractId] =
    new JsonFormat[ContractId] {
      override def write(obj: ContractId) =
        JsString(obj.coid)
      override def read(json: JsValue) = json match {
        case JsString(s) =>
          ContractId.fromString(s).fold(deserializationError(_), identity)
        case _ => deserializationError("ContractId must be a string")
      }
    }
  implicit object ApplicationIdFormat extends JsonFormat[ApplicationId] {
    def read(value: JsValue) = value match {
      case JsString(s) => ApplicationId(s)
      case _ => deserializationError("Expected ApplicationId string")
    }
    def write(id: ApplicationId) = JsString(ApplicationId.unwrap(id))
  }
  implicit val apiParametersFormat = jsonFormat4(ApiParameters)
  implicit val participantsFormat = jsonFormat3(Participants[ApiParameters])
}

// Daml script, either an Action that can be executed immediately, or a
// Function that requires an argument.
sealed abstract class Script extends Product with Serializable
object Script {

  // For now, we do not care of the logging context for Daml-Script, so we create a
  // global dummy context, we can feed the Speedy Machine and the Scenario service with.
  private[script] val DummyLoggingContext: LoggingContext =
    LoggingContext.newLoggingContext(identity)

  final case class Action(expr: SExpr, scriptIds: ScriptIds) extends Script
  final case class Function(expr: SExpr, param: Type, scriptIds: ScriptIds) extends Script {
    def apply(arg: SValue): Script.Action = Script.Action(SEApp(expr, Array(arg)), scriptIds)
  }

  def fromIdentifier(
      compiledPackages: CompiledPackages,
      scriptId: Identifier,
  ): Either[String, Script] = {
    val scriptExpr = SEVal(LfDefRef(scriptId))
    val script = compiledPackages.pkgInterface.lookupValue(scriptId).left.map(_.pretty)
    def getScriptIds(ty: Type): Either[String, ScriptIds] =
      ScriptIds.fromType(ty).toRight(s"Expected type 'Daml.Script.Script a' but got $ty")
    script.flatMap {
      case GenDValue(TApp(TApp(TBuiltin(BTArrow), param), result), _, _) =>
        for {
          scriptIds <- getScriptIds(result)
        } yield Script.Function(scriptExpr, param, scriptIds)
      case GenDValue(ty, _, _) =>
        for {
          scriptIds <- getScriptIds(ty)
        } yield Script.Action(scriptExpr, scriptIds)
    }
  }
}

object Runner {

  final case class InterpretationError(error: SError.SError)
      extends RuntimeException(s"${Pretty.prettyError(error).render(80)}")

  private[script] val compilerConfig = {
    import Compiler._
    Config(
      // FIXME: Should probably not include 1.dev by default.
      allowedLanguageVersions = LanguageVersion.DevVersions,
      packageValidation = FullPackageValidation,
      profiling = NoProfile,
      stacktracing = FullStackTrace,
    )
  }

  val BLANK_APPLICATION_ID: ApplicationId = ApplicationId("")
  val DEFAULT_APPLICATION_ID: ApplicationId = ApplicationId("daml-script")
  private[script] def connectApiParameters(
      params: ApiParameters,
      tlsConfig: TlsConfiguration,
      maxInboundMessageSize: Int,
  )(implicit ec: ExecutionContext, seq: ExecutionSequencerFactory): Future[GrpcLedgerClient] = {
    val applicationId = params.application_id.getOrElse(
      // If an application id was not supplied, but an access token was,
      // we leave the application id empty so that the ledger will
      // determine it from the access token.
      if (params.access_token.nonEmpty) BLANK_APPLICATION_ID else DEFAULT_APPLICATION_ID
    )
    val clientConfig = LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      token = params.access_token,
    )
    val clientChannelConfig = LedgerClientChannelConfiguration(
      sslContext = tlsConfig.client(),
      maxInboundMessageSize = maxInboundMessageSize,
    )
    LedgerClient
      .singleHost(params.host, params.port, clientConfig, clientChannelConfig)
      .map(new GrpcLedgerClient(_, applicationId))
  }
  // We might want to have one config per participant at some point but for now this should be sufficient.
  def connect(
      participantParams: Participants[ApiParameters],
      tlsConfig: TlsConfiguration,
      maxInboundMessageSize: Int,
  )(implicit
      ec: ExecutionContext,
      seq: ExecutionSequencerFactory,
  ): Future[Participants[GrpcLedgerClient]] = {
    for {
      defaultClient <- participantParams.default_participant.traverse(x =>
        connectApiParameters(x, tlsConfig, maxInboundMessageSize)
      )
      participantClients <- participantParams.participants.traverse(v =>
        connectApiParameters(v, tlsConfig, maxInboundMessageSize)
      )
    } yield Participants(defaultClient, participantClients, participantParams.party_participants)
  }

  def jsonClients(
      participantParams: Participants[ApiParameters],
      envSig: EnvironmentSignature,
  )(implicit ec: ExecutionContext, system: ActorSystem): Future[Participants[JsonLedgerClient]] = {
    def client(params: ApiParameters) = {
      val uri = Uri(params.host).withPort(params.port)
      params.access_token match {
        case None =>
          Future.failed(new RuntimeException(s"The JSON API always requires access tokens"))
        case Some(token) =>
          val client = new JsonLedgerClient(uri, Jwt(token), envSig, system)
          if (params.application_id.isDefined && params.application_id != client.applicationId) {
            Future.failed(
              new RuntimeException(
                s"ApplicationId specified in token ${client.applicationId} must match ${params.application_id}"
              )
            )
          } else {
            Future.successful(new JsonLedgerClient(uri, Jwt(token), envSig, system))
          }
      }

    }
    for {
      defClient <- participantParams.default_participant.traverse(client(_))
      otherClients <- participantParams.participants.traverse(client)
    } yield Participants(defClient, otherClients, participantParams.party_participants)
  }

  // Executes a Daml script
  //
  // Looks for the script in the given DAR, applies the input value as an
  // argument if provided, and runs the script with the given participants.
  def run(
      dar: Dar[(PackageId, Package)],
      scriptId: Identifier,
      inputValue: Option[JsValue],
      initialClients: Participants[ScriptLedgerClient],
      timeMode: ScriptTimeMode,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[SValue] = {
    val darMap = dar.all.toMap
    val compiledPackages = PureCompiledPackages.assertBuild(darMap, Runner.compilerConfig)
    def converter(json: JsValue, typ: Type) = {
      val ifaceDar = dar.map(pkg => SignatureReader.readPackageSignature(() => \/-(pkg))._2)
      val envIface = EnvironmentSignature.fromPackageSignatures(ifaceDar)
      Converter.fromJsonValue(
        scriptId.qualifiedName,
        envIface,
        compiledPackages,
        typ,
        json,
      )
    }
    run(
      compiledPackages,
      scriptId,
      Some(converter),
      inputValue,
      initialClients,
      timeMode,
    )._2
  }

  // Executes a Daml script
  //
  // Looks for the script in the given compiledPackages, applies the input
  // value as an argument if provided together with a conversion function,
  // and runs the script with the given participants.
  def run(
      compiledPackages: PureCompiledPackages,
      scriptId: Identifier,
      convertInputValue: Option[(JsValue, Type) => Either[String, SValue]],
      inputValue: Option[JsValue],
      initialClients: Participants[ScriptLedgerClient],
      timeMode: ScriptTimeMode,
      traceLog: TraceLog = Speedy.Machine.newTraceLog,
      warningLog: WarningLog = Speedy.Machine.newWarningLog,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): (Speedy.Machine, Future[SValue]) = {
    val script = data.assertRight(Script.fromIdentifier(compiledPackages, scriptId))
    val scriptAction: Script.Action = (script, inputValue) match {
      case (script: Script.Action, None) => script
      case (script: Script.Function, Some(inputJson)) =>
        convertInputValue match {
          case Some(f) =>
            f(inputJson, script.param) match {
              case Left(msg) => throw new ConverterException(msg)
              case Right(arg) => script.apply(arg)
            }
          case None =>
            throw new RuntimeException(
              s"The script ${scriptId} requires an argument, but a converter was not provided"
            )
        }
      case (_: Script.Action, Some(_)) =>
        throw new RuntimeException(s"The script ${scriptId} does not take arguments.")
      case (_: Script.Function, None) =>
        throw new RuntimeException(s"The script ${scriptId} requires an argument.")
    }
    val runner = new Runner(compiledPackages, scriptAction, timeMode)
    runner.runWithClients(initialClients, traceLog, warningLog)
  }
}

private[lf] class Runner(
    compiledPackages: CompiledPackages,
    script: Script.Action,
    timeMode: ScriptTimeMode,
) extends StrictLogging {

  // We overwrite the definition of fromLedgerValue with an identity function.
  // This is a type error but Speedy doesn’t care about the types and the only thing we do
  // with the result is convert it to ledger values/record so this is safe.
  private val extendedCompiledPackages = {
    val fromLedgerValue: PartialFunction[SDefinitionRef, SDefinition] = {
      case LfDefRef(id) if id == script.scriptIds.damlScript("fromLedgerValue") =>
        SDefinition(SEMakeClo(Array(), 1, SELocA(0)))
    }
    new CompiledPackages(Runner.compilerConfig) {
      override def getDefinition(dref: SDefinitionRef): Option[SDefinition] =
        fromLedgerValue.andThen(Some(_)).applyOrElse(dref, compiledPackages.getDefinition)
      // FIXME: avoid override of non abstract method
      override def pkgInterface: PackageInterface = compiledPackages.pkgInterface
      override def packageIds: collection.Set[PackageId] = compiledPackages.packageIds
      // FIXME: avoid override of non abstract method
      override def definitions: PartialFunction[SDefinitionRef, SDefinition] =
        fromLedgerValue.orElse(compiledPackages.definitions)
    }
  }

  // Maps GHC unit ids to LF package ids. Used for location conversion.
  private val knownPackages: Map[String, PackageId] = (for {
    pkgId <- compiledPackages.packageIds
    md <- compiledPackages.pkgInterface.lookupPackage(pkgId).toOption.flatMap(_.metadata).toList
  } yield (s"${md.name}-${md.version}" -> pkgId)).toMap

  // Returns the machine that will be used for execution as well as a Future for the result.
  def runWithClients(
      initialClients: Participants[ScriptLedgerClient],
      traceLog: TraceLog = Speedy.Machine.newTraceLog,
      warningLog: WarningLog = Speedy.Machine.newWarningLog,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): (Speedy.OffLedgerMachine, Future[SValue]) = {
    val machine =
      Speedy.Machine.fromPureSExpr(
        extendedCompiledPackages,
        script.expr,
        traceLog = traceLog,
        warningLog = warningLog,
      )(Script.DummyLoggingContext)

    def stepToValue(): Either[RuntimeException, SValue] =
      machine.run() match {
        case SResultFinal(v) =>
          Right(v)
        case SResultError(err) =>
          Left(Runner.InterpretationError(err))
        case res =>
          Left(new IllegalStateException(s"Internal error: Unexpected speedy result $res"))
      }

    val env = new ScriptF.Env(
      script.scriptIds,
      timeMode,
      initialClients,
      machine,
    )

    def run(expr: SExpr): Future[SValue] = {
      machine.setExpressionToEvaluate(expr)
      stepToValue()
        .fold(Future.failed, Future.successful)
        .flatMap(fsu => Converter toFuture unrollFree(fsu))
        .flatMap {
          case Right((vv, v)) =>
            Converter
              .toFuture(
                ScriptF.parse(
                  ScriptF.Ctx(knownPackages, extendedCompiledPackages),
                  vv,
                  v,
                )
              )
              .flatMap { scriptF =>
                scriptF match {
                  case ScriptF.Catch(act, handle) =>
                    run(SEAppAtomic(SEValue(act), Array(SEValue(SUnit)))).transformWith {
                      case Success(v) => Future.successful(SEValue(v))
                      case Failure(
                            exce @ Runner.InterpretationError(
                              SError.SErrorDamlException(IE.UnhandledException(typ, value))
                            )
                          ) =>
                        val e =
                          SELet1(
                            SEImportValue(typ, value),
                            SELet1(
                              SEAppAtomic(SEBuiltin(SBToAny(typ)), Array(SELocS(1))),
                              SEAppAtomic(SEValue(handle), Array(SELocS(1))),
                            ),
                          )
                        machine.setExpressionToEvaluate(e)
                        stepToValue()
                          .fold(Future.failed, Future.successful)
                          .flatMap {
                            case SOptional(None) =>
                              Future.failed(exce)
                            case SOptional(Some(free)) => Future.successful(SEValue(free))
                            case e =>
                              Future.failed(
                                new ConverterException(s"Expected SOptional but got $e")
                              )
                          }
                      case Failure(e) => Future.failed(e)
                    }
                  case ScriptF.Throw(SAny(ty, value)) =>
                    Future.failed(
                      Runner.InterpretationError(
                        SError
                          .SErrorDamlException(IE.UnhandledException(ty, value.toUnnormalizedValue))
                      )
                    )
                  case cmd: ScriptF.Cmd =>
                    cmd.execute(env).transform {
                      case Failure(exception) =>
                        Failure(new ScriptF.FailedCmd(cmd, exception))
                      case Success(value) =>
                        Success(value)
                    }
                }
              }
              .flatMap(run(_))
          case Left(v) =>
            v match {
              case SRecord(_, _, ArrayList(newState, _)) => {
                // Unwrap the Tuple2 we get from the inlined StateT.
                Future { newState }
              }
              case _ => Future.failed(new ConverterException(s"Expected Tuple2 but got $v"))
            }
        }
    }

    val resultF = for {
      _ <- Future.unit // We want the evaluation of following stepValue() to happen in a future.
      result <- stepToValue().fold(Future.failed, Future.successful)
      expr <- result match {
        // Unwrap Script type and apply to ()
        // For backwards-compatibility we support the 1 and the 2-field versions.
        case SRecord(_, _, vals) if vals.size == 1 || vals.size == 2 => {
          vals.get(0) match {
            case SPAP(_, _, _) =>
              Future(SEAppAtomic(SEValue(vals.get(0)), Array(SEValue(SUnit))))
            case _ =>
              Future.failed(
                new ConverterException(
                  "Mismatch in structure of Script type. " +
                    "This probably means that you tried to run a script built against an " +
                    "SDK <= 0.13.55-snapshot.20200304.3329.6a1c75cf with a script runner from a newer SDK."
                )
              )
          }
        }
        case v => Future.failed(new ConverterException(s"Expected record with 1 field but got $v"))
      }
      v <- run(expr)
    } yield v
    (machine, resultF)
  }
}
