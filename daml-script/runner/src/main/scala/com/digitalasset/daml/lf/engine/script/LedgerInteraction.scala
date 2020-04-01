// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.unmarshalling._
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import io.grpc.{Status, StatusRuntimeException}
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import scalaz.{-\/, \/-}
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import spray.json._

import com.daml.lf.data.Ref._
import com.daml.lf.data.Ref
import com.daml.lf.iface.{EnvironmentInterface, InterfaceType}
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SValue._
import com.daml.lf.value.Value
import com.daml.lf.value.Value.AbsoluteContractId
import com.daml.jwt.domain.Jwt
import com.daml.jwt.JwtDecoder
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands._
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.validation.ValueValidator
import com.daml.ledger.client.LedgerClient
import com.daml.platform.participant.util.LfEngineToApi.{
  lfValueToApiRecord,
  lfValueToApiValue,
  toApiIdentifier
}

object ScriptLedgerClient {

  sealed trait Command
  final case class CreateCommand(templateId: Identifier, argument: Value[AbsoluteContractId])
      extends Command
  final case class ExerciseCommand(
      templateId: Identifier,
      contractId: AbsoluteContractId,
      choice: String,
      argument: Value[AbsoluteContractId])
      extends Command
  final case class ExerciseByKeyCommand(
      templateId: Identifier,
      key: Value[AbsoluteContractId],
      choice: String,
      argument: Value[AbsoluteContractId])
      extends Command
  final case class CreateAndExerciseCommand(
      templateId: Identifier,
      template: Value[AbsoluteContractId],
      choice: String,
      argument: Value[AbsoluteContractId])
      extends Command

  sealed trait CommandResult
  final case class CreateResult(contractId: AbsoluteContractId) extends CommandResult
  final case class ExerciseResult(
      templateId: Identifier,
      choice: String,
      result: Value[AbsoluteContractId])
      extends CommandResult

  final case class ActiveContract(
      templateId: Identifier,
      contractId: AbsoluteContractId,
      argument: Value[AbsoluteContractId])
}

// This abstracts over the interaction with the ledger. This allows
// us to plug in something that interacts with the JSON API as well as
// something that works against the gRPC API.
trait ScriptLedgerClient {
  def query(party: SParty, templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[Seq[ScriptLedgerClient.ActiveContract]]

  def submit(
      applicationId: ApplicationId,
      party: SParty,
      commands: List[ScriptLedgerClient.Command])(implicit ec: ExecutionContext, mat: Materializer)
    : Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]]

  def allocateParty(partyIdHint: String, displayName: String)(
      implicit ec: ExecutionContext,
      mat: Materializer): Future[SParty]

}

class GrpcLedgerClient(val grpcClient: LedgerClient) extends ScriptLedgerClient {
  override def query(party: SParty, templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    val filter = TransactionFilter(
      List((party.value, Filters(Some(InclusiveFilters(Seq(toApiIdentifier(templateId))))))).toMap)
    val acsResponses =
      grpcClient.activeContractSetClient
        .getActiveContracts(filter, verbose = true)
        .runWith(Sink.seq)
    acsResponses.map(acsPages =>
      acsPages.flatMap(page =>
        page.activeContracts.map(createdEvent => {
          val argument = ValueValidator.validateRecord(createdEvent.getCreateArguments) match {
            case Left(err) => throw new ConverterException(err.toString)
            case Right(argument) => argument
          }
          val cid =
            AbsoluteContractId
              .fromString(createdEvent.contractId)
              .fold(
                err => throw new ConverterException(err),
                identity
              )
          ScriptLedgerClient.ActiveContract(templateId, cid, argument)
        })))
  }

  override def submit(
      applicationId: ApplicationId,
      party: SParty,
      commands: List[ScriptLedgerClient.Command])(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    val ledgerCommands = commands.traverseU(toCommand(_)) match {
      case Left(err) => throw new ConverterException(err)
      case Right(cmds) => cmds
    }
    val apiCommands = Commands(
      party = party.value,
      commands = ledgerCommands,
      ledgerId = grpcClient.ledgerId.unwrap,
      applicationId = applicationId.unwrap,
      commandId = UUID.randomUUID.toString,
    )
    val request = SubmitAndWaitRequest(Some(apiCommands))
    val transactionTreeF = grpcClient.commandServiceClient
      .submitAndWaitForTransactionTree(request)
      .map(Right(_))
      .recoverWith({
        case s: StatusRuntimeException
            // This is used for submit must fail so we only catch ABORTED and INVALID_ARGUMENT.
            // Errors like PERMISSION_DENIED are not caught.
            if s.getStatus.getCode == Status.Code.ABORTED || s.getStatus.getCode == Status.Code.INVALID_ARGUMENT =>
          Future.successful(Left(s))

      })
    transactionTreeF.map(r =>
      r.right.map(transactionTree => {
        val events = transactionTree.getTransaction.rootEventIds
          .map(evId => transactionTree.getTransaction.eventsById(evId))
          .toList
        events.traverseU(fromTreeEvent(_)) match {
          case Left(err) => throw new ConverterException(err)
          case Right(results) => results
        }
      }))
  }

  override def allocateParty(partyIdHint: String, displayName: String)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    grpcClient.partyManagementClient
      .allocateParty(Some(partyIdHint), Some(displayName))
      .map(r => SParty(r.party))
  }

  private def toCommand(command: ScriptLedgerClient.Command): Either[String, Command] =
    command match {
      case ScriptLedgerClient.CreateCommand(templateId, argument) =>
        for {
          arg <- lfValueToApiRecord(true, argument)
        } yield Command().withCreate(CreateCommand(Some(toApiIdentifier(templateId)), Some(arg)))
      case ScriptLedgerClient.ExerciseCommand(templateId, contractId, choice, argument) =>
        for {
          arg <- lfValueToApiValue(true, argument)
        } yield
          Command().withExercise(
            ExerciseCommand(Some(toApiIdentifier(templateId)), contractId.coid, choice, Some(arg)))
      case ScriptLedgerClient.ExerciseByKeyCommand(templateId, key, choice, argument) =>
        for {
          key <- lfValueToApiValue(true, key)
          argument <- lfValueToApiValue(true, argument)
        } yield
          Command().withExerciseByKey(
            ExerciseByKeyCommand(
              Some(toApiIdentifier(templateId)),
              Some(key),
              choice,
              Some(argument)))
      case ScriptLedgerClient.CreateAndExerciseCommand(templateId, template, choice, argument) =>
        for {
          template <- lfValueToApiRecord(true, template)
          argument <- lfValueToApiValue(true, argument)
        } yield
          Command().withCreateAndExercise(
            CreateAndExerciseCommand(
              Some(toApiIdentifier(templateId)),
              Some(template),
              choice,
              Some(argument)))
    }

  private def fromTreeEvent(ev: TreeEvent): Either[String, ScriptLedgerClient.CommandResult] =
    ev match {
      case TreeEvent(TreeEvent.Kind.Created(created)) =>
        for {
          cid <- AbsoluteContractId.fromString(created.contractId)
        } yield ScriptLedgerClient.CreateResult(cid)
      case TreeEvent(TreeEvent.Kind.Exercised(exercised)) =>
        for {
          result <- ValueValidator.validateValue(exercised.getExerciseResult).left.map(_.toString)
          templateId <- Converter.fromApiIdentifier(exercised.getTemplateId)
        } yield ScriptLedgerClient.ExerciseResult(templateId, exercised.choice, result)
      case TreeEvent(TreeEvent.Kind.Empty) =>
        throw new ConverterException("Invalid tree event Empty")
    }
}

// Current limitations and issues when running DAML script over the JSON API:
// 1. Multi-command submissions are not supported. This is simply not possible until
//    we have an endpoint for this in the JSON API.
// 2. This is the biggest issue imho: parties are kind of a mess. `submit` and `query` pretend
//    that you can choose the party you submitting commands as. However, this is not the case
//    for the JSON API since it always infers the party from the JWT (which also means it does
//    not support multi-party tokens). We add a validation step to `submit` and `query` that
//    errors out if the token party does not match the party pased as an argument.

class JsonLedgerClient(
    uri: Uri,
    token: Jwt,
    envIface: EnvironmentInterface,
    actorSystem: ActorSystem)
    extends ScriptLedgerClient {
  import JsonLedgerClient.JsonProtocol._

  private val decodedJwt = JwtDecoder.decode(token) match {
    case -\/(e) => throw new IllegalArgumentException(e.toString)
    case \/-(a) => a
  }
  private val tokenPayload: AuthServiceJWTPayload =
    AuthServiceJWTCodec.readFromString(decodedJwt.payload) match {
      case Failure(e) => throw e
      case Success(s) => s
    }

  implicit val system = actorSystem
  implicit val executionContext = system.dispatcher

  private def damlLfTypeLookup(id: Identifier) =
    envIface.typeDecls.get(id).map(_.`type`)

  override def query(party: SParty, templateId: Identifier)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(uri.path./("v1")./("query")),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        JsonLedgerClient.QueryArgs(templateId).toJson.prettyPrint),
      headers = List(Authorization(OAuth2BearerToken(token.value)))
    )
    for {
      () <- validateTokenParty(party, "query")
      resp <- Http().singleRequest(req)
      queryResponse <- if (resp.status.isSuccess) {
        Unmarshal(resp.entity).to[JsonLedgerClient.QueryResponse]
      } else {
        getResponseDataBytes(resp).flatMap {
          case body => Future.failed(new RuntimeException(s"Failed to query ledger: $resp, $body"))
        }
      }
    } yield {
      val ctx = templateId.qualifiedName
      val ifaceType = Converter.toIfaceType(ctx, TTyCon(templateId)).right.get
      val parsedResults = queryResponse.results.map(r => {
        val payload = r.payload.convertTo[Value[AbsoluteContractId]](
          LfValueCodec.apiValueJsonReader(ifaceType, damlLfTypeLookup(_)))
        val cid = AbsoluteContractId.assertFromString(r.contractId)
        ScriptLedgerClient.ActiveContract(templateId, cid, payload)
      })
      parsedResults
    }
  }
  override def submit(
      applicationId: ApplicationId,
      party: SParty,
      commands: List[ScriptLedgerClient.Command])(implicit ec: ExecutionContext, mat: Materializer)
    : Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]] = {
    for {
      () <- validateTokenParty(party, "submit a command")
      result <- commands match {
        case Nil => Future { Right(List()) }
        case command :: Nil =>
          command match {
            case ScriptLedgerClient.CreateCommand(tplId, argument) =>
              create(tplId, argument)
            case ScriptLedgerClient.ExerciseCommand(tplId, cid, choice, argument) =>
              exercise(tplId, cid, choice, argument)
            case ScriptLedgerClient.ExerciseByKeyCommand(tplId, key, choice, argument) =>
              exerciseByKey(tplId, key, choice, argument)
            case ScriptLedgerClient.CreateAndExerciseCommand(tplId, template, choice, argument) =>
              createAndExercise(tplId, template, choice, argument)
          }
        case _ =>
          Future.failed(
            new RuntimeException(
              "Multi-command submissions are not supported by the HTTP JSON API."))
      }
    } yield result
  }
  override def allocateParty(partyIdHint: String, displayName: String)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(uri.path./("v1")./("parties")./("allocate")),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        JsonLedgerClient.AllocatePartyArgs(partyIdHint, displayName).toJson.prettyPrint),
      headers = List(Authorization(OAuth2BearerToken(token.value)))
    )
    for {
      resp <- Http().singleRequest(req)
      response <- if (resp.status.isSuccess) {
        Unmarshal(resp.entity).to[JsonLedgerClient.AllocatePartyResponse]
      } else {
        getResponseDataBytes(resp).flatMap {
          case body =>
            Future.failed(new RuntimeException(s"Failed to allocate party: $resp, $body"))
        }
      }
    } yield {
      SParty(response.identifier)
    }
  }

  // Check that the party in the token matches the given party.
  private def validateTokenParty(party: SParty, what: String): Future[Unit] = {
    tokenPayload.party match {
      case None =>
        Future.failed(new RuntimeException(
          s"Tried to $what as ${party.value} but token does not provide a unique party identifier"))
      case Some(tokenParty) if (!(tokenParty == party.value)) =>
        Future.failed(
          new RuntimeException(
            s"Tried to $what as ${party.value} but token is only valid for $tokenParty"))
      case _ => Future.unit
    }
  }

  private def create(tplId: Identifier, argument: Value[AbsoluteContractId])
    : Future[Either[StatusRuntimeException, List[ScriptLedgerClient.CreateResult]]] = {
    val ctx = tplId.qualifiedName
    val ifaceType = Converter.toIfaceType(ctx, TTyCon(tplId)).right.get
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    commandRequest[JsonLedgerClient.CreateArgs, JsonLedgerClient.CreateResponse](
      "create",
      JsonLedgerClient.CreateArgs(tplId, jsonArgument))
      .map(_.map {
        case JsonLedgerClient.CreateResponse(cid) =>
          List(ScriptLedgerClient.CreateResult(AbsoluteContractId.assertFromString(cid)))
      })
  }

  private def exercise(
      tplId: Identifier,
      contractId: AbsoluteContractId,
      choice: String,
      argument: Value[AbsoluteContractId])
    : Future[Either[StatusRuntimeException, List[ScriptLedgerClient.ExerciseResult]]] = {
    val ctx = tplId.qualifiedName
    val choiceDef = envIface
      .typeDecls(tplId)
      .asInstanceOf[InterfaceType.Template]
      .template
      .choices(Name.assertFromString(choice))
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    commandRequest[JsonLedgerClient.ExerciseArgs, JsonLedgerClient.ExerciseResponse](
      "exercise",
      JsonLedgerClient.ExerciseArgs(tplId, contractId, choice, jsonArgument))
      .map(_.map {
        case JsonLedgerClient.ExerciseResponse(result) =>
          List(
            ScriptLedgerClient.ExerciseResult(
              tplId,
              choice,
              result.convertTo[Value[AbsoluteContractId]](
                LfValueCodec.apiValueJsonReader(choiceDef.returnType, damlLfTypeLookup(_)))))
      })
  }

  private def exerciseByKey(
      tplId: Identifier,
      key: Value[AbsoluteContractId],
      choice: String,
      argument: Value[AbsoluteContractId])
    : Future[Either[StatusRuntimeException, List[ScriptLedgerClient.ExerciseResult]]] = {
    val ctx = tplId.qualifiedName
    val choiceDef = envIface
      .typeDecls(tplId)
      .asInstanceOf[InterfaceType.Template]
      .template
      .choices(Name.assertFromString(choice))
    val jsonKey = LfValueCodec.apiValueToJsValue(key)
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    commandRequest[JsonLedgerClient.ExerciseByKeyArgs, JsonLedgerClient.ExerciseResponse](
      "exercise",
      JsonLedgerClient
        .ExerciseByKeyArgs(tplId, jsonKey, choice, jsonArgument)).map(_.map {
      case JsonLedgerClient.ExerciseResponse(result) =>
        List(
          ScriptLedgerClient.ExerciseResult(
            tplId,
            choice,
            result.convertTo[Value[AbsoluteContractId]](
              LfValueCodec.apiValueJsonReader(choiceDef.returnType, damlLfTypeLookup(_)))))
    })
  }

  private def createAndExercise(
      tplId: Identifier,
      template: Value[AbsoluteContractId],
      choice: String,
      argument: Value[AbsoluteContractId])
    : Future[Either[StatusRuntimeException, List[ScriptLedgerClient.CommandResult]]] = {
    val ctx = tplId.qualifiedName
    val choiceDef = envIface
      .typeDecls(tplId)
      .asInstanceOf[InterfaceType.Template]
      .template
      .choices(Name.assertFromString(choice))
    val jsonTemplate = LfValueCodec.apiValueToJsValue(template)
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    commandRequest[
      JsonLedgerClient.CreateAndExerciseArgs,
      JsonLedgerClient.CreateAndExerciseResponse](
      "create-and-exercise",
      JsonLedgerClient
        .CreateAndExerciseArgs(tplId, jsonTemplate, choice, jsonArgument))
      .map(_.map {
        case JsonLedgerClient.CreateAndExerciseResponse(cid, result) =>
          List(
            ScriptLedgerClient
              .CreateResult(AbsoluteContractId.assertFromString(cid)): ScriptLedgerClient.CommandResult,
            ScriptLedgerClient.ExerciseResult(
              tplId,
              choice,
              result.convertTo[Value[AbsoluteContractId]](
                LfValueCodec.apiValueJsonReader(choiceDef.returnType, damlLfTypeLookup(_))))
          )
      })
  }

  def getResponseDataBytes(
      resp: HttpResponse)(implicit mat: Materializer, ec: ExecutionContext): Future[String] = {
    val fb = resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a).map(_.utf8String)
    fb
  }

  def commandRequest[In, Out](endpoint: String, argument: In)(
      implicit argumentWriter: JsonWriter[In],
      outputReader: RootJsonReader[Out]): Future[Either[StatusRuntimeException, Out]] = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(uri.path./("v1")./(endpoint)),
      entity = HttpEntity(ContentTypes.`application/json`, argument.toJson.prettyPrint),
      headers = List(Authorization(OAuth2BearerToken(token.value)))
    )
    Http().singleRequest(req).flatMap { resp =>
      if (resp.status.isSuccess) {
        Unmarshal(resp.entity).to[Out].map(Right(_))
      } else if (resp.status == StatusCodes.InternalServerError) {
        // TODO (MK) Using a grpc exception here doesn’t make that much sense.
        // We should refactor this to provide something more general.
        getResponseDataBytes(resp).map(description =>
          Left(new StatusRuntimeException(Status.UNKNOWN.withDescription(description))))
      } else {
        // A non-500 failure is something like invalid JSON. In that case
        // the script runner is just broken so fail hard.
        Future.failed(new RuntimeException(s"Request failed: $resp"))
      }
    }
  }
}

object JsonLedgerClient {
  final case class QueryArgs(templateId: Identifier)
  final case class QueryResponse(results: List[ActiveContract])
  final case class ActiveContract(contractId: String, payload: JsValue)

  final case class CreateArgs(templateId: Identifier, payload: JsValue)
  final case class CreateResponse(contractId: String)

  final case class ExerciseArgs(
      templateId: Identifier,
      contractId: AbsoluteContractId,
      choice: String,
      argument: JsValue)
  final case class ExerciseResponse(result: JsValue)

  final case class ExerciseByKeyArgs(
      templateId: Identifier,
      key: JsValue,
      choice: String,
      argument: JsValue)

  final case class CreateAndExerciseArgs(
      templateId: Identifier,
      payload: JsValue,
      choice: String,
      argument: JsValue)
  final case class CreateAndExerciseResponse(contractId: String, result: JsValue)

  final case class AllocatePartyArgs(
      identifierHint: String,
      displayName: String
  )
  final case class AllocatePartyResponse(identifier: Ref.Party)

  object JsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
    implicit val identifierWriter: JsonWriter[Identifier] = identifier =>
      JsString(
        identifier.packageId + ":" + identifier.qualifiedName.module.toString + ":" + identifier.qualifiedName.name.toString)

    implicit val queryWriter: JsonWriter[QueryArgs] = args =>
      JsObject("templateIds" -> JsArray(identifierWriter.write(args.templateId)))
    implicit val queryReader: RootJsonReader[QueryResponse] = v => {
      v.asJsObject.getFields("result") match {
        case Seq(JsArray(results)) => QueryResponse(results.toList.map(_.convertTo[ActiveContract]))
        case _ => deserializationError(s"Could not parse QueryResponse: $v")
      }
    }
    implicit val activeContractReader: RootJsonReader[ActiveContract] = v => {
      v.asJsObject.getFields("contractId", "payload") match {
        case Seq(JsString(s), v) => ActiveContract(s, v)
        case _ => deserializationError(s"Could not parse ActiveContract: $v")
      }
    }

    implicit val createWriter: JsonWriter[CreateArgs] = args =>
      JsObject("templateId" -> args.templateId.toJson, "payload" -> args.payload)
    implicit val createReader: RootJsonReader[CreateResponse] = v => {
      v.asJsObject.getFields("result") match {
        case Seq(result) =>
          result.asJsObject.getFields("contractId") match {
            case Seq(JsString(cid)) => CreateResponse(cid)
            case _ => deserializationError(s"Could not parse CreateResponse: $v")
          }
        case _ => deserializationError(s"Could not parse CreateResponse: $v")
      }
    }

    implicit val exerciseWriter: JsonWriter[ExerciseArgs] = args =>
      JsObject(
        "templateId" -> args.templateId.toJson,
        "contractId" -> args.contractId.coid.toString.toJson,
        "choice" -> args.choice.toJson,
        "argument" -> args.argument)
    implicit val exerciseByKeyWriter: JsonWriter[ExerciseByKeyArgs] = args =>
      JsObject(
        "templateId" -> args.templateId.toJson,
        "key" -> args.key,
        "choice" -> args.choice.toJson,
        "argument" -> args.argument)
    implicit val exerciseReader: RootJsonReader[ExerciseResponse] = v => {
      v.asJsObject.getFields("result") match {
        case Seq(result) =>
          result.asJsObject.getFields("exerciseResult") match {
            case Seq(result) => ExerciseResponse(result)
            case _ => deserializationError(s"Could not parse ExerciseResponse: $v")
          }
        case _ => deserializationError(s"Could not parse ExerciseResponse: $v")
      }
    }

    implicit val createAndExerciseWriter: JsonWriter[CreateAndExerciseArgs] = args =>
      JsObject(
        "templateId" -> args.templateId.toJson,
        "payload" -> args.payload,
        "choice" -> args.choice.toJson,
        "argument" -> args.argument)
    implicit val createAndExerciseReader: RootJsonReader[CreateAndExerciseResponse] = v => {
      v.asJsObject.getFields("result") match {
        case Seq(result) =>
          result.asJsObject.getFields("exerciseResult", "events") match {
            case Seq(result, events) =>
              events match {
                case JsArray(events) if events.size >= 1 =>
                  events.head.asJsObject.getFields("created") match {
                    case Seq(created) =>
                      created.asJsObject.getFields("contractId") match {
                        case Seq(JsString(cid)) => CreateAndExerciseResponse(cid, result)
                        case _ =>
                          deserializationError(s"Could not parse CreateAndExerciseResponse: $v")
                      }
                    case _ => deserializationError(s"Could not parse CreateAndExerciseResponse: $v")
                  }
                case _ => deserializationError(s"Could not parse CreateAndExerciseResponse: $v")
              }
            case _ => deserializationError(s"Could not parse CreateAndExerciseResponse: $v")
          }
        case _ => deserializationError(s"Could not parse CreateAndExerciseResponse: $v")
      }
    }

    implicit val allocatePartyWriter: JsonFormat[AllocatePartyArgs] = jsonFormat2(AllocatePartyArgs)
    implicit val allocatePartyReader: RootJsonReader[AllocatePartyResponse] = v =>
      v.asJsObject.getFields("result") match {
        case Seq(result) =>
          result.asJsObject.getFields("identifier") match {
            case Seq(JsString(identifier)) =>
              AllocatePartyResponse(Ref.Party.assertFromString(identifier))
            case _ => deserializationError(s"Could not parse AllocatePartyResponse: $v")
          }
        case _ => deserializationError(s"Could not parse AllocatePartyResponse: $v")
    }
  }
}
