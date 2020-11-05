// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import java.time.Clock

import scala.concurrent.{ExecutionContext, Future}
import scalaz.{Applicative, NonEmptyList, OneAnd, Traverse, \/-}
import scalaz.std.either._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.map._
import scalaz.std.scalaFuture._
import scalaz.std.set._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._

import scala.language.higherKinds
import spray.json._
import com.daml.lf.archive.Dar
import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref._
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.iface.EnvironmentInterface
import com.daml.lf.iface.reader.InterfaceReader
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.{Compiler, Pretty, SExpr, SDefinition, SValue, Speedy}
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client.configuration.{CommandClientConfiguration, LedgerIdRequirement}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.LedgerClientConfiguration
import ParticipantsJsonProtocol.ContractIdFormat
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.VersionTimeline
import com.daml.script.converter.Converter.{JavaList, toContractId, unrollFree}
import com.daml.script.converter.ConverterException

object LfValueCodec extends ApiCodecCompressed[ContractId](false, false)

case class Participant(participant: String)
case class ApiParameters(
    host: String,
    port: Int,
    access_token: Option[String],
    application_id: Option[ApplicationId])
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
      participant <- if (participants.all(_ == participants.head)) {
        Right(participants.head)
      } else {
        Left(
          s"All parties must be on the same participant but parties were allocated as follows: ${parties.toList
            .zip(participants.toList)}")
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
      Participants[B](fa.default_participant.map(f), fa.participants.map({
        case (k, v) => (k, f(v))
      }), fa.party_participants)

    override def traverseImpl[G[_]: Applicative, A, B](fa: Participants[A])(
        f: A => G[B]): G[Participants[B]] = {
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
          ContractId fromString s fold (deserializationError(_), identity)
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
      allowedLanguageVersions = VersionTimeline.devLanguageVersions,
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
      maxInboundMessageSize: Int)(
      implicit ec: ExecutionContext,
      seq: ExecutionSequencerFactory): Future[Participants[GrpcLedgerClient]] = {
    for {
      defaultClient <- participantParams.default_participant.traverse(x =>
        connectApiParameters(x, tlsConfig, maxInboundMessageSize))
      participantClients <- participantParams.participants.traverse(v =>
        connectApiParameters(v, tlsConfig, maxInboundMessageSize))
    } yield Participants(defaultClient, participantClients, participantParams.party_participants)
  }

  def jsonClients(participantParams: Participants[ApiParameters], envIface: EnvironmentInterface)(
      implicit ec: ExecutionContext,
      system: ActorSystem): Future[Participants[JsonLedgerClient]] = {
    def client(params: ApiParameters) = {
      val uri = Uri(params.host).withPort(params.port)
      params.access_token match {
        case None =>
          Future.failed(new RuntimeException(s"The JSON API always requires access tokens"))
        case Some(token) =>
          val client = new JsonLedgerClient(uri, Jwt(token), envIface, system)
          if (params.application_id.isDefined && params.application_id != client.tokenPayload.applicationId) {
            Future.failed(new RuntimeException(
              s"ApplicationId specified in token ${client.tokenPayload.applicationId} must match ${params.application_id}"))
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
      timeMode: ScriptTimeMode)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): Future[SValue] = {
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
            inputJson) match {
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

  // used to help scalac propagate the expected `B` type into `f`, which doesn't
  // work with the literal tuple syntax.  The curried form also indents much
  // more nicely with scalafmt, with far less horizontal space used
  private def m2c[B, Z](expect: String)(f: B PartialFunction Z): (String, B PartialFunction Z) =
    (expect, f)

  // a variant of match2 specifically for our run function that lets us
  // put the unique error message content right next to the pattern that
  // must have failed
  private def match2[A, B, Z](a: A, b: B)(
      f: A => (String, B PartialFunction Future[Z])): Future[Z] = {
    val (expect, bz) = f(a)
    bz.applyOrElse(
      b,
      (b: B) => Future failed (new ConverterException(s"Expected $expect but got $b")))
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

  private val utcClock = Clock.systemUTC()

  private def lookupChoiceTy(id: Identifier, choice: Name): Either[String, Type] =
    for {
      pkg <- compiledPackages
        .getSignature(id.packageId)
        .toRight(s"Failed to find package ${id.packageId}")
      module <- pkg.modules
        .get(id.qualifiedName.module)
        .toRight(s"Failed to find module ${id.qualifiedName.module}")
      tpl <- module.templates
        .get(id.qualifiedName.name)
        .toRight(s"Failed to find template ${id.qualifiedName.name}")
      choice <- tpl.choices
        .get(choice)
        .toRight(s"Failed to find choice $choice in $id")
    } yield choice.returnType

  private val valueTranslator = new preprocessing.ValueTranslator(extendedCompiledPackages)

  // Maps GHC unit ids to LF package ids. Used for location conversion.
  private val knownPackages: Map[String, PackageId] = (for {
    pkgId <- compiledPackages.packageIds
    md <- compiledPackages.getSignature(pkgId).flatMap(_.metadata).toList
  } yield (s"${md.name}-${md.version}" -> pkgId)).toMap

  // Returns the machine that will be used for execution as well as a Future for the result.
  def runWithClients(initialClients: Participants[ScriptLedgerClient])(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): (Speedy.Machine, Future[SValue]) = {
    var clients = initialClients
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

    // Copy the tracelog from the client to the current machine
    // interleaving ledger-side trace statements with client-side trace
    // statements.
    def copyTracelog(client: ScriptLedgerClient) = {
      for ((msg, optLoc) <- client.tracelogIterator) {
        machine.traceLog.add(msg, optLoc)
      }
      client.clearTracelog
    }

    def run(expr: SExpr): Future[SValue] = {
      machine.setExpressionToEvaluate(expr)
      import Runner.{match2, m2c}
      stepToValue()
        .fold(Future.failed, Future.successful)
        .flatMap(fsu => Converter toFuture unrollFree(fsu))
        .flatMap {
          case Right((vv, v)) =>
            match2(vv, v) {
              case "Submit" =>
                m2c("record with 3 or 4 fields") {
                  // For backwards compatibility we support SubmitCmd without a callstack.
                  case SRecord(
                      _,
                      _,
                      JavaList(sParty, SRecord(_, _, JavaList(freeAp)), continue, restVals @ _*))
                      if restVals.size <= 1 =>
                    for {
                      party <- Converter.toFuture(Converter
                        .toParty(sParty))
                      commands <- Converter.toFuture(Converter
                        .toCommands(extendedCompiledPackages, freeAp))
                      client <- Converter.toFuture(clients
                        .getPartyParticipant(party))
                      commitLocation <- restVals.headOption cata (sLoc =>
                        Converter.toFuture(Converter.toOptionLocation(knownPackages, sLoc)),
                      Future(None))
                      submitRes <- client.submit(party, commands, commitLocation)
                      _ = copyTracelog(client)
                      v <- submitRes match {
                        case Right(results) => {
                          for {
                            filled <- Converter.toFuture(
                              Converter
                                .fillCommandResults(
                                  extendedCompiledPackages,
                                  lookupChoiceTy,
                                  valueTranslator,
                                  freeAp,
                                  results))
                            v <- {
                              run(filled)
                            }
                          } yield v
                        }
                        case Left(statusEx) => {
                          // This branch is superseded by SubmitMustFail below,
                          // however, it is maintained for backwards
                          // compatibility with DAML script DARs generated by
                          // older SDK versions that didn't distinguish Submit
                          // and SubmitMustFail.
                          for {
                            res <- Converter.toFuture(Converter
                              .fromStatusException(script.scriptIds, statusEx))
                            v <- {
                              run(SEApp(SEValue(continue), Array(SEValue(res))))
                            }
                          } yield v
                        }
                      }
                    } yield v

                }
              case "SubmitMustFail" =>
                m2c("record with 3 or 4 fields") {
                  // For backwards compatibility we support SubmitCmd without a callstack.
                  case SRecord(
                      _,
                      _,
                      JavaList(sParty, SRecord(_, _, JavaList(freeAp)), continue, restVals @ _*))
                      if restVals.size <= 1 =>
                    for {
                      party <- Converter.toFuture(Converter
                        .toParty(sParty))
                      commands <- Converter.toFuture(Converter
                        .toCommands(extendedCompiledPackages, freeAp))
                      client <- Converter.toFuture(clients
                        .getPartyParticipant(party))
                      commitLocation <- restVals.headOption cata (sLoc =>
                        Converter.toFuture(Converter.toOptionLocation(knownPackages, sLoc)),
                      Future(None))
                      submitRes <- client.submitMustFail(party, commands, commitLocation)
                      _ = copyTracelog(client)
                      v <- submitRes match {
                        case Right(()) =>
                          run(SEApp(SEValue(continue), Array(SEValue(SUnit))))
                        case Left(()) =>
                          Future.failed(
                            new DamlEUserError("Expected submit to fail but it succeeded"))
                      }
                    } yield v
                }
              case "Query" =>
                m2c("record with 3 fields") {
                  case SRecord(_, _, JavaList(sParties, sTplId, continue)) =>
                    for {
                      parties <- Converter.toFuture(Converter
                        .toParties(sParties))
                      tplId <- Converter.toFuture(Converter
                        .typeRepToIdentifier(sTplId))
                      client <- Converter.toFuture(clients
                        .getPartiesParticipant(parties))
                      acs <- client.query(parties, tplId)
                      res <- Converter.toFuture(
                        FrontStack(acs)
                          .traverse(Converter
                            .fromCreated(valueTranslator, _)))
                      v <- {
                        run(SEApp(SEValue(continue), Array(SEValue(SList(res)))))
                      }
                    } yield v
                }
              case "AllocParty" =>
                m2c("record with 4 fields") {
                  case SRecord(
                      _,
                      _,
                      JavaList(
                        SText(displayName),
                        SText(partyIdHint),
                        SOptional(sParticipantName),
                        continue)) =>
                    for {
                      participantName <- sParticipantName match {
                        case Some(SText(t)) => Future.successful(Some(Participant(t)))
                        case None => Future.successful(None)
                        case v =>
                          Future.failed(
                            new ConverterException(s"Expected Option(SText) but got $v"))
                      }
                      client <- clients.getParticipant(participantName) match {
                        case Right(client) => Future.successful(client)
                        case Left(err) => Future.failed(new RuntimeException(err))
                      }
                      party <- client.allocateParty(partyIdHint, displayName)
                      v <- {
                        participantName match {
                          case None => {
                            // If no participant is specified, we use default_participant so we don’t need to change anything.
                          }
                          case Some(participant) =>
                            clients = clients
                              .copy(
                                party_participants = clients.party_participants + (party -> participant))
                        }
                        run(SEApp(SEValue(continue), Array(SEValue(SParty(party)))))
                      }
                    } yield v
                }
              case "ListKnownParties" =>
                m2c("record with 2 fields") {
                  case SRecord(_, _, JavaList(SOptional(sParticipantName), continue)) => {
                    for {
                      participantName <- sParticipantName match {
                        case Some(SText(t)) => Future.successful(Some(Participant(t)))
                        case None => Future.successful(None)
                        case v =>
                          Future.failed(
                            new ConverterException(s"Expected Option(SText) but got $v"))
                      }
                      client <- clients.getParticipant(participantName) match {
                        case Right(client) => Future.successful(client)
                        case Left(err) => Future.failed(new RuntimeException(err))
                      }
                      partyDetails <- client.listKnownParties()
                      partyDetails_ <- Converter.toFuture(partyDetails.traverse(details =>
                        Converter.fromPartyDetails(script.scriptIds, details)))
                      v <- {
                        run(
                          SEApp(
                            SEValue(continue),
                            Array(SEValue(SList(FrontStack(partyDetails_))))))
                      }
                    } yield v
                  }
                }
              case "GetTime" =>
                m2c("a function") {
                  case continue =>
                    for {
                      time <- timeMode match {
                        case ScriptTimeMode.Static => {
                          // We don’t parametrize this by participant since this
                          // is only useful in static time mode and using the time
                          // service with multiple participants is very dodgy.
                          for {
                            client <- Converter.toFuture(clients.getParticipant(None))
                            t <- client.getStaticTime()
                          } yield t
                        }
                        case ScriptTimeMode.WallClock =>
                          Future {
                            Timestamp.assertFromInstant(utcClock.instant())
                          }
                      }
                      v <- run(SEApp(SEValue(continue), Array(SEValue(STimestamp(time)))))

                    } yield v

                }
              case "SetTime" =>
                m2c("SetTimePayload") {
                  case SRecord(_, _, JavaList(sT, continue)) =>
                    timeMode match {
                      case ScriptTimeMode.Static =>
                        for {
                          // We don’t parametrize this by participant since this
                          // is only useful in static time mode and using the time
                          // service with multiple participants is very dodgy.
                          client <- Converter.toFuture(clients.getParticipant(None))
                          t <- Converter.toFuture(Converter.toTimestamp(sT))
                          _ <- client.setStaticTime(t)
                          v <- run(SEApp(SEValue(continue), Array(SEValue(SUnit))))
                        } yield v
                      case ScriptTimeMode.WallClock =>
                        Future.failed(
                          new RuntimeException("setTime is not supported in wallclock mode"))

                    }
                }
              case "Sleep" =>
                m2c("record with 2 fields") {
                  case SRecord(
                      _,
                      _,
                      JavaList(SRecord(_, _, JavaList(SInt64(sleepMicros))), continue)) =>
                    val sleepMillis = sleepMicros / 1000
                    val sleepNanos = (sleepMicros % 1000) * 1000
                    Thread.sleep(sleepMillis, sleepNanos.toInt)
                    run(SEApp(SEValue(continue), Array(SEValue(SUnit))))
                }
              case "QueryContractId" =>
                m2c("record with 4 fields") {
                  case SRecord(_, _, JavaList(sParty, sTplId, sCid, continue)) =>
                    for {
                      parties <- Converter.toFuture(Converter.toParties(sParty))
                      tplId <- Converter.toFuture(Converter.typeRepToIdentifier(sTplId))
                      cid <- Converter.toFuture(toContractId(sCid))
                      client <- Converter.toFuture(clients.getPartyParticipant(parties.head))
                      optR <- client.queryContractId(parties, tplId, cid)
                      optR <- Converter.toFuture(
                        optR.traverse(Converter.fromContract(valueTranslator, _)))
                      v <- run(SEApp(SEValue(continue), Array(SEValue(SOptional(optR)))))
                    } yield v
                }
              case "QueryContractKey" =>
                m2c("record with 4 fields") {
                  case SRecord(_, _, JavaList(sParties, sTplId, sKey, continue)) =>
                    for {
                      parties <- Converter.toFuture(Converter.toParties(sParties))
                      tplId <- Converter.toFuture(Converter.typeRepToIdentifier(sTplId))
                      key <- Converter.toFuture(Converter.toAnyContractKey(sKey))
                      client <- Converter.toFuture(clients.getPartiesParticipant(parties))
                      optR <- client.queryContractKey(parties, tplId, key.key)
                      optR <- Converter.toFuture(
                        optR.traverse(Converter.fromCreated(valueTranslator, _)))
                      v <- run(SEApp(SEValue(continue), Array(SEValue(SOptional(optR)))))
                    } yield v
                }
              case _ => m2c("Submit, Query or AllocParty")(PartialFunction.empty)
            }
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
                    "SDK <= 0.13.55-snapshot.20200304.3329.6a1c75cf with a script runner from a newer SDK."))
          }
        }
        case v => Future.failed(new ConverterException(s"Expected record with 1 field but got $v"))
      }
      v <- run(expr)
    } yield v
    (machine, resultF)
  }
}
