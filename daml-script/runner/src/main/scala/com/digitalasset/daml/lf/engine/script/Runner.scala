// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.lf.iface.EnvironmentInterface
import com.daml.lf.iface.reader.InterfaceReader
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{Compiler, Pretty, SDefinition, SError, SExpr, SValue, Speedy}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.script.converter.Converter.{JavaList, unrollFree}
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

object LfValueCodec extends ApiCodecCompressed[ContractId](false, false)

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
  def getPartyParticipant(party: Party): Either[String, T] =
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

// DAML script, either an Action that can be executed immediately, or a
// Function that requires an argument.
sealed abstract class Script extends Product with Serializable
object Script {
  final case class Action(expr: SExpr, scriptIds: ScriptIds) extends Script
  final case class Function(expr: SExpr, param: Type, scriptIds: ScriptIds) extends Script {
    def apply(arg: SExpr): Script.Action = Script.Action(SEApp(expr, Array(arg)), scriptIds)
  }

  def fromIdentifier(
      compiledPackages: CompiledPackages,
      scriptId: Identifier,
  ): Either[String, Script] = {
    val scriptExpr = SEVal(LfDefRef(scriptId))
    val scriptTy = compiledPackages
      .getSignature(scriptId.packageId)
      .flatMap(_.lookupDefinition(scriptId.qualifiedName).toOption) match {
      case Some(DValueSignature(ty, _, _, _)) => Right(ty)
      case Some(d @ DTypeSyn(_, _)) => Left(s"Expected DAML script but got synonym $d")
      case Some(d @ DDataType(_, _, _)) => Left(s"Expected DAML script but got datatype $d")
      case None => Left(s"Could not find DAML script $scriptId")
    }
    def getScriptIds(ty: Type): Either[String, ScriptIds] =
      ScriptIds.fromType(ty).toRight(s"Expected type 'Daml.Script.Script a' but got $ty")
    scriptTy.flatMap {
      case TApp(TApp(TBuiltin(BTArrow), param), result) =>
        for {
          scriptIds <- getScriptIds(result)
        } yield Script.Function(scriptExpr, param, scriptIds)
      case ty =>
        for {
          scriptIds <- getScriptIds(ty)
        } yield Script.Action(scriptExpr, scriptIds)
    }
  }
}

object Runner {

  private val compilerConfig = {
    import Compiler._
    Config(
      // FIXME: Should probably not include 1.dev by default.
      allowedLanguageVersions = LanguageVersion.DevVersions,
      packageValidation = FullPackageValidation,
      profiling = NoProfile,
      stacktracing = FullStackTrace,
    )
  }

  val DEFAULT_APPLICATION_ID: ApplicationId = ApplicationId("daml-script")
  private def connectApiParameters(
      params: ApiParameters,
      tlsConfig: TlsConfiguration,
      maxInboundMessageSize: Int,
  )(implicit ec: ExecutionContext, seq: ExecutionSequencerFactory): Future[GrpcLedgerClient] = {
    val applicationId = params.application_id.getOrElse(Runner.DEFAULT_APPLICATION_ID)
    val clientConfig = LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      sslContext = tlsConfig.client,
      token = params.access_token,
      maxInboundMessageSize = maxInboundMessageSize,
    )
    LedgerClient
      .singleHost(params.host, params.port, clientConfig)
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
      envIface: EnvironmentInterface,
  )(implicit ec: ExecutionContext, system: ActorSystem): Future[Participants[JsonLedgerClient]] = {
    def client(params: ApiParameters) = {
      val uri = Uri(params.host).withPort(params.port)
      params.access_token match {
        case None =>
          Future.failed(new RuntimeException(s"The JSON API always requires access tokens"))
        case Some(token) =>
          val client = new JsonLedgerClient(uri, Jwt(token), envIface, system)
          if (
            params.application_id.isDefined && params.application_id != client.tokenPayload.applicationId
          ) {
            Future.failed(
              new RuntimeException(
                s"ApplicationId specified in token ${client.tokenPayload.applicationId} must match ${params.application_id}"
              )
            )
          } else {
            Future.successful(new JsonLedgerClient(uri, Jwt(token), envIface, system))
          }
      }

    }
    for {
      defClient <- participantParams.default_participant.traverse(client(_))
      otherClients <- participantParams.participants.traverse(client)
    } yield Participants(defClient, otherClients, participantParams.party_participants)
  }

  // Executes a DAML script
  //
  // Looks for the script in the given DAR, applies the input value as an
  // argument if provided, and runs the script with the given pariticipants.
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
    val compiledPackages = data.assertRight(PureCompiledPackages(darMap, Runner.compilerConfig))
    val script = data.assertRight(Script.fromIdentifier(compiledPackages, scriptId))
    val scriptAction: Script.Action = (script, inputValue) match {
      case (script: Script.Action, None) => script
      case (script: Script.Function, Some(inputJson)) =>
        val ifaceDar = dar.map(pkg => InterfaceReader.readInterface(() => \/-(pkg))._2)
        val envIface = EnvironmentInterface.fromReaderInterfaces(ifaceDar)
        val arg = Converter
          .fromJsonValue(
            scriptId.qualifiedName,
            envIface,
            compiledPackages,
            script.param,
            inputJson,
          ) match {
          case Left(msg) => throw new ConverterException(msg)
          case Right(x) => x
        }
        script.apply(SEValue(arg))
      case (_: Script.Action, Some(_)) =>
        throw new RuntimeException(s"The script ${scriptId} does not take arguments.")
      case (_: Script.Function, None) =>
        throw new RuntimeException(s"The script ${scriptId} requires an argument.")
    }
    val runner = new Runner(compiledPackages, scriptAction, timeMode)
    runner.runWithClients(initialClients)._2
  }
}

class Runner(compiledPackages: CompiledPackages, script: Script.Action, timeMode: ScriptTimeMode)
    extends StrictLogging {

  // We overwrite the definition of fromLedgerValue with an identity function.
  // This is a type error but Speedy doesn’t care about the types and the only thing we do
  // with the result is convert it to ledger values/record so this is safe.
  private val extendedCompiledPackages = {
    val fromLedgerValue: PartialFunction[SDefinitionRef, SDefinition] = {
      case LfDefRef(id) if id == script.scriptIds.damlScript("fromLedgerValue") =>
        SDefinition(SEMakeClo(Array(), 1, SELocA(0)))
    }
    new CompiledPackages(Runner.compilerConfig) {
      override def getSignature(pkgId: PackageId): Option[PackageSignature] =
        compiledPackages.getSignature(pkgId)
      override def getDefinition(dref: SDefinitionRef): Option[SDefinition] =
        fromLedgerValue.andThen(Some(_)).applyOrElse(dref, compiledPackages.getDefinition)
      // FIXME: avoid override of non abstract method
      override def signatures: PartialFunction[PackageId, PackageSignature] =
        compiledPackages.signatures
      override def packageIds: Set[PackageId] = compiledPackages.packageIds
      // FIXME: avoid override of non abstract method
      override def definitions: PartialFunction[SDefinitionRef, SDefinition] =
        fromLedgerValue.orElse(compiledPackages.definitions)
      override def packageLanguageVersion: PartialFunction[PackageId, LanguageVersion] =
        compiledPackages.packageLanguageVersion
    }
  }

  // Maps GHC unit ids to LF package ids. Used for location conversion.
  private val knownPackages: Map[String, PackageId] = (for {
    pkgId <- compiledPackages.packageIds
    md <- compiledPackages.getSignature(pkgId).flatMap(_.metadata).toList
  } yield (s"${md.name}-${md.version}" -> pkgId)).toMap

  // Returns the machine that will be used for execution as well as a Future for the result.
  def runWithClients(initialClients: Participants[ScriptLedgerClient])(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): (Speedy.Machine, Future[SValue]) = {
    val machine =
      Speedy.Machine.fromPureSExpr(extendedCompiledPackages, script.expr)

    def stepToValue(): Either[RuntimeException, SValue] =
      machine.run() match {
        case SResultFinalValue(v) =>
          Right(v)
        case SResultError(err) =>
          logger.error(Pretty.prettyError(err).render(80))
          Left(err)
        case res =>
          Left(new RuntimeException(s"Unexpected speedy result $res"))
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
              .toFuture(ScriptF.parse(ScriptF.Ctx(knownPackages, extendedCompiledPackages), vv, v))
              .flatMap { scriptF =>
                scriptF match {
                  case ScriptF.Catch(act, handle) =>
                    run(SEApp(SEValue(act), Array(SEValue(SUnit)))).transformWith {
                      case Success(v) => Future.successful(SEValue(v))
                      case Failure(SError.DamlEUnhandledException(exc)) =>
                        machine.setExpressionToEvaluate(SEApp(SEValue(handle), Array(SEValue(exc))))
                        Converter
                          .toFuture(stepToValue())
                          .flatMap {
                            case SOptional(None) =>
                              Future.failed(SError.DamlEUnhandledException(exc))
                            case SOptional(Some(free)) => Future.successful(SEValue(free))
                            case e =>
                              Future.failed(
                                new ConverterException(s"Expected SOptional but got $e")
                              )
                          }
                      case Failure(e) => Future.failed(e)
                    }
                  case ScriptF.Throw(exc) =>
                    Future.failed(SError.DamlEUnhandledException(exc))
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
              case SRecord(_, _, JavaList(newState, _)) => {
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
              Future(SEApp(SEValue(vals.get(0)), Array(SEValue(SUnit))))
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
