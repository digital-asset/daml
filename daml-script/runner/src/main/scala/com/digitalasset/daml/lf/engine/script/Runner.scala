// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package script

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import java.util.UUID

import io.grpc.netty.NettyChannelBuilder
import scala.concurrent.{ExecutionContext, Future}
import scalaz.\/-
import scalaz.std.either._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import spray.json._
import com.daml.api.util.TimeProvider
import com.daml.lf.archive.Dar
import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref._
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.iface.EnvironmentInterface
import com.daml.lf.iface.reader.InterfaceReader
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.{Compiler, InitialSeeding, Pretty, SExpr, SValue, Speedy}
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value.AbsoluteContractId
import com.daml.lf.value.json.ApiCodecCompressed
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands._
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.LedgerClientConfiguration
import com.google.protobuf.duration.Duration
import ParticipantsJsonProtocol.AbsoluteContractIdFormat

object LfValueCodec extends ApiCodecCompressed[AbsoluteContractId](false, false)

case class Participant(participant: String)
case class Party(party: String)
case class ApiParameters(host: String, port: Int)
case class Participants[+T](
    default_participant: Option[T],
    participants: Map[Participant, T],
    party_participants: Map[Party, Participant],
) {
  def getPartyParticipant(party: Party): Either[String, T] =
    party_participants.get(party) match {
      case None =>
        default_participant.fold[Either[String, T]](
          Left(s"No participant for party $party and no default participant"))(Right(_))
      case Some(participant) => getParticipant(Some(participant))
    }
  def getParticipant(participantOpt: Option[Participant]): Either[String, T] =
    participantOpt match {
      case None =>
        default_participant.fold[Either[String, T]](Left(s"No default participant"))(Right(_))
      case Some(participant) =>
        participants.get(participant) match {
          case None =>
            default_participant.fold[Either[String, T]](
              Left(s"No participant $participant and no default participant"))(Right(_))
          case Some(t) => Right(t)
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
      case JsString(s) => Party(s)
      case _ => deserializationError("Expected Party string")
    }
    def write(p: Party) = JsString(p.party)
  }
  implicit val AbsoluteContractIdFormat: JsonFormat[AbsoluteContractId] =
    new JsonFormat[AbsoluteContractId] {
      override def write(obj: AbsoluteContractId) =
        JsString(obj.coid)
      override def read(json: JsValue) = json match {
        case JsString(s) =>
          AbsoluteContractId fromString s fold (deserializationError(_), identity)
        case _ => deserializationError("ContractId must be a string")
      }
    }
  implicit val apiParametersFormat = jsonFormat2(ApiParameters)
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
      scriptId: Identifier): Either[String, Script] = {
    val scriptExpr = SEVal(LfDefRef(scriptId))
    val scriptTy = compiledPackages
      .getPackage(scriptId.packageId)
      .flatMap(_.lookupIdentifier(scriptId.qualifiedName).toOption) match {
      case Some(DValue(ty, _, _, _)) => Right(ty)
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
  private def connectApiParameters(
      params: ApiParameters,
      clientConfig: LedgerClientConfiguration,
      maxInboundMessageSize: Int)(
      implicit ec: ExecutionContext,
      seq: ExecutionSequencerFactory): Future[GrpcLedgerClient] = {
    LedgerClient
      .fromBuilder(
        NettyChannelBuilder
          .forAddress(params.host, params.port)
          .maxInboundMessageSize(maxInboundMessageSize),
        clientConfig,
      )
      .map(new GrpcLedgerClient(_))
//    LedgerClient.singleHost(params.host, params.port, clientConfig).map(new GrpcLedgerClient(_))
  }
  // We might want to have one config per participant at some point but for now this should be sufficient.
  def connect(
      participantParams: Participants[ApiParameters],
      clientConfig: LedgerClientConfiguration,
      maxInboundMessageSize: Int)(
      implicit ec: ExecutionContext,
      seq: ExecutionSequencerFactory): Future[Participants[GrpcLedgerClient]] = {
    for {
      // The standard library is incredibly weird. Option is not Traversable so we have to convert to a list and back.
      // Map is but it doesn’t return a Map so we have to call toMap afterwards.
      defaultClient <- Future
        .traverse(participantParams.default_participant.toList)(x =>
          connectApiParameters(x, clientConfig, maxInboundMessageSize))
        .map(_.headOption)
      participantClients <- Future
        .traverse(participantParams.participants: Map[Participant, ApiParameters])({
          case (k, v) => connectApiParameters(v, clientConfig, maxInboundMessageSize).map((k, _))
        })
        .map(_.toMap)
    } yield Participants(defaultClient, participantClients, participantParams.party_participants)
  }

  def jsonClients(
      participantParams: Participants[ApiParameters],
      token: String,
      envIface: EnvironmentInterface)(
      implicit ec: ExecutionContext,
      system: ActorSystem): Future[Participants[JsonLedgerClient]] = {
    def client(params: ApiParameters) = {
      val uri = Uri(params.host + ":" + params.port.toString)
      new JsonLedgerClient(uri, Jwt(token), envIface, system)
    }
    val defClient = participantParams.default_participant.map(client(_))
    val otherClients = participantParams.participants.map({ case (k, v) => (k, client(v)) })
    Future { Participants(defClient, otherClients, participantParams.party_participants) }
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
      applicationId: ApplicationId,
      timeProvider: TimeProvider)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[SValue] = {
    val darMap = dar.all.toMap
    val compiledPackages = PureCompiledPackages(darMap).right.get
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
      case (script: Script.Action, Some(_)) =>
        throw new RuntimeException(s"The script ${scriptId} does not take arguments.")
      case (script: Script.Function, None) =>
        throw new RuntimeException(s"The script ${scriptId} requires an argument.")
    }
    val runner =
      new Runner(compiledPackages, scriptAction, applicationId, timeProvider)
    runner.runWithClients(initialClients)
  }
}

class Runner(
    compiledPackages: CompiledPackages,
    script: Script.Action,
    applicationId: ApplicationId,
    timeProvider: TimeProvider)
    extends StrictLogging {

  private def lookupChoiceTy(id: Identifier, choice: Name): Either[String, Type] =
    for {
      pkg <- compiledPackages
        .getPackage(id.packageId)
        .fold[Either[String, Package]](Left(s"Failed to find package ${id.packageId}"))(Right(_))
      module <- pkg.modules
        .get(id.qualifiedName.module)
        .fold[Either[String, Module]](Left(s"Failed to find module ${id.qualifiedName.module}"))(
          Right(_))
      definition <- module.definitions
        .get(id.qualifiedName.name)
        .fold[Either[String, Definition]](Left(s"Failed to find ${id.qualifiedName.name}"))(
          Right(_))
      tpl <- definition match {
        case DDataType(_, _, DataRecord(_, Some(tpl))) => Right(tpl)
        case _ => Left(s"Expected template definition but got $definition")
      }
      choice <- tpl.choices
        .get(choice)
        .fold[Either[String, TemplateChoice]](Left(s"Failed to find choice $choice in $id"))(
          Right(_))
    } yield choice.returnType

  // We overwrite the definition of fromLedgerValue with an identity function.
  // This is a type error but Speedy doesn’t care about the types and the only thing we do
  // with the result is convert it to ledger values/record so this is safe.
  private val extendedCompiledPackages = {
    val fromLedgerValue: PartialFunction[SDefinitionRef, SExpr] = {
      case LfDefRef(id) if id == script.scriptIds.damlScript("fromLedgerValue") =>
        SEMakeClo(Array(), 1, SELocA(0))
    }
    new CompiledPackages {
      def getPackage(pkgId: PackageId): Option[Package] = compiledPackages.getPackage(pkgId)
      def getDefinition(dref: SDefinitionRef): Option[SExpr] =
        fromLedgerValue.andThen(Some(_)).applyOrElse(dref, compiledPackages.getDefinition)
      override def packages = compiledPackages.packages
      def packageIds = compiledPackages.packageIds
      override def definitions = fromLedgerValue.orElse(compiledPackages.definitions)
      override def profilingMode = Compiler.NoProfile
    }
  }
  private val valueTranslator = new preprocessing.ValueTranslator(extendedCompiledPackages)

  private def toSubmitRequest(ledgerId: LedgerId, party: SParty, cmds: Seq[Command]) = {
    val commands = Commands(
      party = party.value,
      commands = cmds,
      ledgerId = ledgerId.unwrap,
      applicationId = applicationId.unwrap,
      commandId = UUID.randomUUID.toString,
      deduplicationTime = Some(Duration(30))
    )
    SubmitAndWaitRequest(Some(commands))
  }

  def runWithClients(initialClients: Participants[ScriptLedgerClient])(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[SValue] = {
    var clients = initialClients
    val machine =
      Speedy.Machine.fromSExpr(
        sexpr = script.expr,
        compiledPackages = extendedCompiledPackages,
        submissionTime = Timestamp.now(),
        seeding = InitialSeeding.NoSeed,
        Set.empty,
      )

    def stepToValue(): Either[RuntimeException, SValue] =
      machine.run() match {
        case SResultFinalValue(v) =>
          Right(v)
        case SResultError(err) =>
          logger.error(Pretty.prettyError(err, machine.ptx).render(80))
          Left(err)
        case res =>
          Left(new RuntimeException(s"Unexpected speedy result $res"))
      }

    def run(expr: SExpr): Future[SValue] = {
      machine.setExpressionToEvaluate(expr)
      stepToValue()
        .fold(Future.failed, Future.successful)
        .flatMap {
          case SVariant(_, "Free", _, v) => {
            v match {
              case SVariant(_, "Submit", _, v) => {
                v match {
                  case SRecord(_, _, vals) if vals.size == 3 => {
                    for {
                      freeAp <- vals.get(1) match {
                        // Unwrap Commands newtype
                        case SRecord(_, _, vals) if vals.size == 1 =>
                          Future.successful(vals.get(0))
                        case v =>
                          Future.failed(
                            new ConverterException(s"Expected record with 1 field but got $v"))
                      }
                      party <- Converter.toFuture(
                        Converter
                          .toParty(vals.get(0)))
                      commands <- Converter.toFuture(
                        Converter
                          .toCommands(extendedCompiledPackages, freeAp))
                      client <- Converter.toFuture(
                        clients
                          .getPartyParticipant(Party(party.value)))
                      submitRes <- client.submit(applicationId, party, commands)
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
                          for {
                            res <- Converter.toFuture(
                              Converter
                                .fromStatusException(script.scriptIds, statusEx))
                            v <- {
                              run(SEApp(SEValue(vals.get(2)), Array(SEValue(res))))
                            }
                          } yield v
                        }
                      }
                    } yield v
                  }
                  case _ =>
                    Future.failed(
                      new ConverterException(s"Expected record with 2 fields but got $v"))
                }
              }
              case SVariant(_, "Query", _, v) => {
                v match {
                  case SRecord(_, _, vals) if vals.size == 3 => {
                    val continue = vals.get(2)
                    for {
                      party <- Converter.toFuture(
                        Converter
                          .toParty(vals.get(0)))
                      tplId <- Converter.toFuture(
                        Converter
                          .typeRepToIdentifier(vals.get(1)))
                      client <- Converter.toFuture(
                        clients
                          .getPartyParticipant(Party(party.value)))
                      acs <- client.query(party, tplId)
                      res <- Converter.toFuture(
                        FrontStack(acs)
                          .traverseU(Converter
                            .fromCreated(valueTranslator, _)))
                      v <- {
                        run(SEApp(SEValue(continue), Array(SEValue(SList(res)))))
                      }
                    } yield v
                  }
                  case _ =>
                    Future.failed(
                      new ConverterException(s"Expected record with 3 fields but got $v"))
                }
              }
              case SVariant(_, "AllocParty", _, v) => {
                v match {
                  case SRecord(_, _, vals) if vals.size == 4 => {
                    val continue = vals.get(3)
                    for {
                      displayName <- vals.get(0) match {
                        case SText(value) => Future.successful(value)
                        case v =>
                          Future.failed(new ConverterException(s"Expected SText but got $v"))
                      }
                      partyIdHint <- vals.get(1) match {
                        case SText(t) => Future.successful(t)
                        case v =>
                          Future.failed(new ConverterException(s"Expected SText but got $v"))
                      }
                      participantName <- vals.get(2) match {
                        case SOptional(Some(SText(t))) => Future.successful(Some(Participant(t)))
                        case SOptional(None) => Future.successful(None)
                        case v =>
                          Future.failed(
                            new ConverterException(s"Expected SOptional(SText) but got $v"))
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
                            clients =
                              clients.copy(
                                party_participants = clients.party_participants + (Party(
                                  party.value) -> participant))
                        }
                        run(SEApp(SEValue(continue), Array(SEValue(party))))
                      }
                    } yield v
                  }
                  case _ =>
                    Future.failed(
                      new ConverterException(s"Expected record with 2 fields but got $v"))
                }
              }
              case SVariant(_, "GetTime", _, continue) => {
                val t = Timestamp.assertFromInstant(timeProvider.getCurrentTime)
                run(SEApp(SEValue(continue), Array(SEValue(STimestamp(t)))))
              }
              case SVariant(_, "Sleep", _, v) => {
                v match {
                  case SRecord(_, _, vals) if vals.size == 2 => {
                    val continue = vals.get(1)
                    for {
                      sleepMicros <- vals.get(0) match {
                        case SRecord(_, _, vals) if vals.size == 1 =>
                          vals.get(0) match {
                            case SInt64(i) => Future.successful(i)
                            case _ =>
                              Future.failed(new ConverterException(s"Expected SInt64 but got $v"))
                          }
                        case v =>
                          Future.failed(new ConverterException(s"Expected RelTime but got $v"))
                      }
                      v <- {
                        val sleepMillis = sleepMicros / 1000
                        val sleepNanos = (sleepMicros % 1000) * 1000
                        Thread.sleep(sleepMillis, sleepNanos.toInt)
                        run(SEApp(SEValue(continue), Array(SEValue(SUnit))))
                      }
                    } yield v
                  }
                  case _ =>
                    Future.failed(
                      new ConverterException(s"Expected record with 2 fields but got $v"))
                }
              }
              case _ =>
                Future.failed(
                  new ConverterException(s"Expected Submit, Query or AllocParty but got $v"))
            }
          }
          case SVariant(_, "Pure", _, v) =>
            v match {
              case SRecord(_, _, vals) if vals.size == 2 => {
                // Unwrap the Tuple2 we get from the inlined StateT.
                Future { vals.get(0) }
              }
              case _ => Future.failed(new ConverterException(s"Expected Tuple2 but got $v"))
            }
          case v => Future.failed(new ConverterException(s"Expected Free or Pure but got $v"))
        }
    }

    for {
      _ <- Future.unit // We want the evaluation of following stepValue() to happen in a future.
      result <- stepToValue().fold(Future.failed, Future.successful)
      expr <- result match {
        // Unwrap Script newtype and apply to ()
        case SRecord(_, _, vals) if vals.size == 1 => {
          vals.get(0) match {
            case SPAP(_, _, _) =>
              Future(SEApp(SEValue(vals.get(0)), Array(SEValue(SUnit))))
            case v =>
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
  }
}
