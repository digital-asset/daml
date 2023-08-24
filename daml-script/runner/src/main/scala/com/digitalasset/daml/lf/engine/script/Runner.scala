// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  IdeLedgerClient,
  ScriptLedgerClient,
}
import com.daml.lf.typesig.EnvironmentSignature
import com.daml.lf.typesig.reader.SignatureReader
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.scenario.{ScenarioLedger, ScenarioRunner}
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.{Compiler, Pretty, SError, SValue, Speedy, Profile, TraceLog, WarningLog}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.logging.LoggingContext
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

import com.daml.lf.archive.ArchivePayload

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

  def map[A](f: T => A): Participants[A] =
    copy(
      default_participant = default_participant.map(f),
      participants = participants.transform((_, v) => f(v)),
    )
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

  trait FailableCmd {
    def stackTrace: StackTrace
    // Human-readable description of the command used in error messages.
    def description: String
  }

  final class FailedCmd(val cmd: FailableCmd, val cause: Throwable)
      extends RuntimeException(
        s"""Command ${cmd.description} failed: ${cause.getMessage}
          |Daml stacktrace:
          |${cmd.stackTrace.pretty()}""".stripMargin,
        cause,
      )
}

object Runner {

  final case class InterpretationError(error: SError.SError)
      extends RuntimeException(s"${Pretty.prettyError(error).render(80)}")

  final case object CanceledByRequest extends RuntimeException
  final case object TimedOut extends RuntimeException

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

  def ideLedgerClient(
      compiledPackages: PureCompiledPackages,
      traceLog: TraceLog,
      warningLog: WarningLog,
  ): Future[Participants[IdeLedgerClient]] =
    Future.successful(
      Participants(
        default_participant =
          Some(new IdeLedgerClient(compiledPackages, traceLog, warningLog, () => false)),
        participants = Map.empty,
        party_participants = Map.empty,
      )
    )

  trait IdeLedgerContext {
    def currentSubmission: Option[ScenarioRunner.CurrentSubmission]
    def ledger: ScenarioLedger
  }

  sealed trait LinkingBehaviour extends Product with Serializable
  object LinkingBehaviour {
    final case object NoLinking extends LinkingBehaviour
    final case object LinkRecent extends LinkingBehaviour
    final case class LinkSpecific(script: Dar[ArchivePayload]) extends LinkingBehaviour
  }

  sealed trait TypeCheckingBehaviour extends Product with Serializable
  object TypeCheckingBehaviour {
    final case object NoTypeChecking extends TypeCheckingBehaviour
    final case class TypeChecking(originalDar: Dar[(PackageId, Package)])
        extends TypeCheckingBehaviour
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
      Some(converter(_, _)),
      inputValue,
      initialClients,
      timeMode,
    )
  }

  // Executes a Daml script
  //
  // Looks for the script in the given compiledPackages, applies the input
  // value as an argument if provided together with a conversion function,
  // and runs the script with the given participants.
  def run[X](
      compiledPackages: PureCompiledPackages,
      scriptId: Identifier,
      convertInputValue: Option[(X, Type) => Either[String, SValue]],
      inputValue: Option[X],
      initialClients: Participants[ScriptLedgerClient],
      timeMode: ScriptTimeMode,
      traceLog: TraceLog = Speedy.Machine.newTraceLog,
      warningLog: WarningLog = Speedy.Machine.newWarningLog,
      profile: Profile = Speedy.Machine.newProfile,
      canceled: () => Option[RuntimeException] = () => None,
      linkingBehaviour: Option[LinkingBehaviour] = None,
      typeCheckingBehaviour: TypeCheckingBehaviour = TypeCheckingBehaviour.NoTypeChecking,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[SValue] =
    runWithOptionalIdeContext(
      compiledPackages,
      scriptId,
      convertInputValue,
      inputValue,
      initialClients,
      timeMode,
      traceLog,
      warningLog,
      profile,
      canceled,
      linkingBehaviour,
      typeCheckingBehaviour,
    )._1

  // Same as run above but requires use of IdeLedgerClient, gives additional context back
  def runIdeLedgerClient[X](
      compiledPackages: PureCompiledPackages,
      scriptId: Identifier,
      convertInputValue: Option[(X, Type) => Either[String, SValue]],
      inputValue: Option[X],
      initialClient: IdeLedgerClient,
      timeMode: ScriptTimeMode,
      traceLog: TraceLog = Speedy.Machine.newTraceLog,
      warningLog: WarningLog = Speedy.Machine.newWarningLog,
      profile: Profile = Speedy.Machine.newProfile,
      canceled: () => Option[RuntimeException] = () => None,
      linkingBehaviour: Option[LinkingBehaviour] = None,
      typeCheckingBehaviour: TypeCheckingBehaviour = TypeCheckingBehaviour.NoTypeChecking,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): (Future[SValue], IdeLedgerContext) = {
    val initialClients = Participants(Some(initialClient), Map.empty, Map.empty)
    val (resultF, oIdeLedgerContext) = runWithOptionalIdeContext(
      compiledPackages,
      scriptId,
      convertInputValue,
      inputValue,
      initialClients,
      timeMode,
      traceLog,
      warningLog,
      profile,
      canceled,
      linkingBehaviour,
      typeCheckingBehaviour,
    )
    (resultF, oIdeLedgerContext.get)
  }

  private def runWithOptionalIdeContext[X](
      compiledPackages: PureCompiledPackages,
      scriptId: Identifier,
      convertInputValue: Option[(X, Type) => Either[String, SValue]],
      inputValue: Option[X],
      initialClients: Participants[ScriptLedgerClient],
      timeMode: ScriptTimeMode,
      traceLog: TraceLog,
      warningLog: WarningLog,
      profile: Profile,
      canceled: () => Option[RuntimeException],
      linkingBehaviour: Option[LinkingBehaviour],
      typeCheckingBehaviour: TypeCheckingBehaviour,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): (Future[SValue], Option[IdeLedgerContext]) = {
    val script = data.assertRight(Script.fromIdentifier(compiledPackages, scriptId))
    val scriptAction: Script.Action = (script, inputValue) match {
      case (script: Script.Action, None) => script
      case (script: Script.Function, Some(input)) =>
        convertInputValue match {
          case Some(f) =>
            f(input, script.param) match {
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
    runner.runWithClients(
      initialClients,
      traceLog,
      warningLog,
      profile,
      canceled,
      linkingBehaviour,
      typeCheckingBehaviour,
    )
  }
}

private[lf] class Runner(
    val compiledPackages: CompiledPackages,
    val script: Script.Action,
    val timeMode: ScriptTimeMode,
) extends StrictLogging {
  import Runner._

  // Maps GHC unit ids to LF package ids. Used for location conversion.
  val knownPackages: Map[String, PackageId] = (for {
    pkgId <- compiledPackages.packageIds
    md <- compiledPackages.pkgInterface.lookupPackage(pkgId).toOption.flatMap(_.metadata).toList
  } yield (s"${md.name}-${md.version}" -> pkgId)).toMap

  def getPackageName(pkgId: PackageId): Option[String] =
    compiledPackages.pkgInterface
      .lookupPackage(pkgId)
      .toOption
      .flatMap(_.metadata)
      .map(meta => meta.name.toString)

  def runWithClients(
      initialClients: Participants[ScriptLedgerClient],
      traceLog: TraceLog = Speedy.Machine.newTraceLog,
      warningLog: WarningLog = Speedy.Machine.newWarningLog,
      profile: Profile = Speedy.Machine.newProfile,
      canceled: () => Option[RuntimeException] = () => None,
      linkingBehaviour: Option[LinkingBehaviour] = None,
      typeCheckingBehaviour: TypeCheckingBehaviour = TypeCheckingBehaviour.NoTypeChecking,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): (Future[SValue], Option[Runner.IdeLedgerContext]) = {
    val damlScriptName = getPackageName(script.scriptIds.scriptPackageId)

    damlScriptName.getOrElse(
      throw new IllegalArgumentException("Couldn't get daml script package name")
    ) match {
      case "daml-script" => {
        // Default (and only) daml2-script linking is NoLinking
        val realLinkingBehaviour = linkingBehaviour.getOrElse(LinkingBehaviour.NoLinking)
        if (realLinkingBehaviour != LinkingBehaviour.NoLinking)
          throw new IllegalArgumentException("Daml Script v1 does not support dynamic linking")
        if (typeCheckingBehaviour != TypeCheckingBehaviour.NoTypeChecking)
          throw new IllegalArgumentException(
            "Daml Script v1 does not support dynamic type checking"
          )
        new v1.Runner(this).runWithClients(initialClients, traceLog, warningLog, profile, canceled)
      }
      case "daml3-script" => {
        // Default daml3-script linking is LinkRecent
        val realLinkingBehaviour = linkingBehaviour.getOrElse(LinkingBehaviour.LinkRecent)
        new v2.Runner(
          this,
          initialClients,
          traceLog,
          warningLog,
          profile,
          canceled,
          realLinkingBehaviour,
          typeCheckingBehaviour,
        ).getResult()
      }
      case pkgName =>
        throw new IllegalArgumentException(
          "Invalid daml script package name. Expected daml-script or daml3-script, got " + pkgName
        )
    }
  }
}
