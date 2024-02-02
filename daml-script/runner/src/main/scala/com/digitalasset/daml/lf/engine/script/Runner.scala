// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.jwt.domain.Jwt
import com.digitalasset.canton.ledger.api.tls.TlsConfiguration
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ParticipantsJsonProtocol.ContractIdFormat
import com.daml.lf.engine.script.ledgerinteraction.{
  GrpcLedgerClient,
  IdeLedgerClient,
  JsonLedgerClient,
  ScriptLedgerClient,
}
import com.daml.lf.engine.script.v2.ledgerinteraction.grpcLedgerClient.AdminLedgerClient
import com.daml.lf.typesig.EnvironmentSignature
import com.daml.lf.typesig.reader.SignatureReader
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LanguageMajorVersion, PackageInterface}
import com.daml.lf.language.LanguageVersionRangeOps._
import com.daml.lf.scenario.{ScenarioLedger, ScenarioRunner}
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.{
  Compiler,
  Pretty,
  Profile,
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
import com.daml.script.converter.ConverterException
import com.digitalasset.canton.logging.NamedLoggerFactory
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

object LfValueCodec extends ApiCodecCompressed(false, false)

case class Participant(participant: String)
case class ApiParameters(
    host: String,
    port: Int,
    access_token: Option[String],
    application_id: Option[Option[Ref.ApplicationId]],
    adminPort: Option[Int] = None,
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
  implicit object ApplicationIdFormat extends JsonFormat[Option[Ref.ApplicationId]] {
    def read(value: JsValue) = value match {
      case JsString(s) => Some(s).filter(_.nonEmpty).map(Ref.ApplicationId.assertFromString)
      case _ => deserializationError("Expected ApplicationId string")
    }
    def write(id: Option[Ref.ApplicationId]) = JsString(id.getOrElse(""))
  }
  implicit val apiParametersFormat: RootJsonFormat[ApiParameters] = jsonFormat5(ApiParameters)
  implicit val participantsFormat: RootJsonFormat[Participants[ApiParameters]] = jsonFormat3(
    Participants[ApiParameters]
  )
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

  final case class FailedCmd(description: String, stackTrace: StackTrace, cause: Throwable)
      extends RuntimeException(
        s"""Command ${description} failed: ${cause.getMessage}
          |Daml stacktrace:
          |${stackTrace.pretty()}""".stripMargin,
        cause,
      )
}

object Runner {

  final case class InterpretationError(error: SError.SError)
      extends RuntimeException(s"${Pretty.prettyError(error).render(80)}")

  final case object CanceledByRequest extends RuntimeException
  final case object TimedOut extends RuntimeException

  private[script] def compilerConfig(majorLanguageVersion: LanguageMajorVersion) = {
    import Compiler._
    Config(
      allowedLanguageVersions =
        VersionRange(min = majorLanguageVersion.minStableVersion, max = majorLanguageVersion.dev),
      packageValidation = FullPackageValidation,
      profiling = NoProfile,
      stacktracing = FullStackTrace,
    )
  }

  val namedLoggerFactory: NamedLoggerFactory = NamedLoggerFactory("daml-script", "")

  val BLANK_APPLICATION_ID: Option[Ref.ApplicationId] = None
  val DEFAULT_APPLICATION_ID: Option[Ref.ApplicationId] = Some(
    Ref.ApplicationId.assertFromString("daml-script")
  )
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
      applicationId = applicationId.getOrElse(""),
      commandClient = CommandClientConfiguration.default,
      token = params.access_token,
    )
    val clientChannelConfig = LedgerClientChannelConfiguration(
      sslContext = tlsConfig.client(),
      maxInboundMessageSize = maxInboundMessageSize,
    )
    LedgerClient
      .singleHost(params.host, params.port, clientConfig, clientChannelConfig, namedLoggerFactory)
      .map(
        new GrpcLedgerClient(
          _,
          applicationId,
          params.adminPort.map(p =>
            AdminLedgerClient.singleHost(params.host, p, clientConfig.token, clientChannelConfig)
          ),
        )
      )
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
          if (
            params.application_id.isDefined && params.application_id.map(
              _.getOrElse("")
            ) != client.applicationId
          ) {
            Future.failed(
              new RuntimeException(
                s"ApplicationId specified in token ${client.applicationId} must match ${params.application_id
                    .map(_.getOrElse(""))}"
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
    val majorVersion = dar.main._2.languageVersion.major
    val compiledPackages =
      PureCompiledPackages.assertBuild(darMap, Runner.compilerConfig(majorVersion))
    def convert(json: JsValue, typ: Type) = {
      val ifaceDar = dar.map(pkg => SignatureReader.readPackageSignature(() => \/-(pkg))._2)
      val envIface = EnvironmentSignature.fromPackageSignatures(ifaceDar)
      Converter(majorVersion).fromJsonValue(
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
      Some(convert(_, _)),
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
      enableContractUpgrading: Boolean = false,
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
      enableContractUpgrading,
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
      enableContractUpgrading: Boolean = false,
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
    val runner = new Runner(compiledPackages, scriptAction, timeMode, enableContractUpgrading)
    runner.runWithClients(initialClients, traceLog, warningLog, profile, canceled)
  }

  def getPackageName(compiledPackages: CompiledPackages, pkgId: PackageId): Option[String] =
    compiledPackages.pkgInterface
      .lookupPackage(pkgId)
      .toOption
      .map(_.metadata)
      .map(meta => meta.name.toString)
}

private[lf] class Runner(
    val compiledPackages: CompiledPackages,
    val script: Script.Action,
    val timeMode: ScriptTimeMode,
    val enableContractUpgrading: Boolean = false,
) extends StrictLogging {

  // We overwrite the definition of 'fromLedgerValue' with an identity function.
  // This is a type error but Speedy doesn’t care about the types and the only thing we do
  // with the result is convert it to ledger values/record so this is safe.
  // We do the same substitution for 'castCatchPayload' to circumvent Daml's
  // lack of existential types.
  val extendedCompiledPackages = {
    val damlScriptDefs: PartialFunction[SDefinitionRef, SDefinition] = {
      // Daml3 script
      // Generalised version of the various unsafe casts we need in daml scripts,
      // casting various types involving LedgerValue to/from their real types.
      case LfDefRef(id)
          if id == script.scriptIds.damlScriptModule(
            "Daml.Script.Internal.LowLevel",
            "dangerousCast",
          ) =>
        SDefinition(SEMakeClo(Array(), 1, SELocA(0)))
      // Daml script legacy
      case LfDefRef(id) if id == script.scriptIds.damlScript("fromLedgerValue") =>
        SDefinition(SEMakeClo(Array(), 1, SELocA(0)))
      case LfDefRef(id) if id == script.scriptIds.damlScript("castCatchPayload") =>
        SDefinition(SEMakeClo(Array(), 1, SELocA(0)))
    }
    new CompiledPackages(
      Runner.compilerConfig(compiledPackages.compilerConfig.allowedLanguageVersions.majorVersion)
    ) {
      override def getDefinition(dref: SDefinitionRef): Option[SDefinition] =
        damlScriptDefs.andThen(Some(_)).applyOrElse(dref, compiledPackages.getDefinition)
      // FIXME: avoid override of non abstract method
      override def pkgInterface: PackageInterface = compiledPackages.pkgInterface
      override def packageIds: collection.Set[PackageId] = compiledPackages.packageIds
      // FIXME: avoid override of non abstract method
      override def definitions: PartialFunction[SDefinitionRef, SDefinition] =
        damlScriptDefs.orElse(compiledPackages.definitions)
    }
  }

  // Maps GHC unit ids to LF package ids. Used for location conversion.
  val knownPackages: Map[String, PackageId] = (for {
    pkgId <- compiledPackages.packageIds
    md <- compiledPackages.pkgInterface.lookupPackage(pkgId).toOption.map(_.metadata).toList
  } yield (s"${md.name}-${md.version}" -> pkgId)).toMap

  def runWithClients(
      initialClients: Participants[ScriptLedgerClient],
      traceLog: TraceLog = Speedy.Machine.newTraceLog,
      warningLog: WarningLog = Speedy.Machine.newWarningLog,
      profile: Profile = Speedy.Machine.newProfile,
      canceled: () => Option[RuntimeException] = () => None,
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): (Future[SValue], Option[Runner.IdeLedgerContext]) = {
    val damlScriptName = Runner.getPackageName(compiledPackages, script.scriptIds.scriptPackageId)

    damlScriptName.getOrElse(
      throw new IllegalArgumentException("Couldn't get daml script package name")
    ) match {
      case "daml-script" =>
        if (enableContractUpgrading)
          throw new IllegalArgumentException("daml2-script does not support Upgrades natively.")
        new v1.Runner(this).runWithClients(initialClients, traceLog, warningLog, profile, canceled)
      case "daml3-script" =>
        new v2.Runner(this, initialClients, traceLog, warningLog, profile, canceled).getResult()
      case pkgName =>
        throw new IllegalArgumentException(
          "Invalid daml script package name. Expected daml-script or daml3-script, got " + pkgName
        )
    }
  }
}
