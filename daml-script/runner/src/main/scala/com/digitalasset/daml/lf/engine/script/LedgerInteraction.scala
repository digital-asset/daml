// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.unmarshalling._
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import io.grpc.StatusRuntimeException
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import spray.json._

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.iface.{EnvironmentInterface, InterfaceType}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands._
import com.digitalasset.ledger.api.v1.transaction.TreeEvent
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.api.validation.ValueValidator
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.platform.participant.util.LfEngineToApi.{
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
          val cid = ContractIdString.fromString(createdEvent.contractId) match {
            case Left(err) => throw new ConverterException(err)
            case Right(cid) => AbsoluteContractId(cid)
          }
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
      .recover({ case s: StatusRuntimeException => Left(s) })
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
          cid <- ContractIdString.fromString(created.contractId)
        } yield ScriptLedgerClient.CreateResult(AbsoluteContractId(cid))
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
// 2. Party allocation is not yet implemented. While this would be possible, it’s not
//    all that useful. For local testing, there is no reason to run DAML Script over the
//    JSON API (and it wouldn’t work properly due to the requirement for JWTs) and for
//    interacting with a production ledger, you usually don’t need it. But it’s simple enough
//    that we might implement it anyway.
// 3. This is the biggest issue imho: parties are kind of a mess. `submit` and `query` pretend
//    that you can choose the party you submitting commands as. However, this is not the case
//    for the JSON API since it always infers the party from the JWT (which also means it does
//    not support multi-party tokens). I don’t have a great answer for this. For interacting
//    with a production ledger, this is probably not super important since you usually act as
//    only one party there. So at least initially, we’re probably best off by just adding validation
//    that ensures that the party you pass to `submit` and `query` matches.
// 4. `submitMustFail` is not yet supported. No fundamental reason for this but it’s also not
//    very useful in a production ledger.
class JsonLedgerClient(
    uri: Uri,
    token: String,
    envIface: EnvironmentInterface,
    actorSystem: ActorSystem)
    extends ScriptLedgerClient {
  import JsonLedgerClient.JsonProtocol._

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
      headers = List(Authorization(OAuth2BearerToken(token)))
    )
    Http()
      .singleRequest(req)
      .flatMap { resp =>
        if (resp.status.isSuccess) {
          Unmarshal(resp.entity).to[JsonLedgerClient.QueryResponse]
        } else {
          throw new RuntimeException(s"Failed to query ledger: $resp")
        }
      }
      .map {
        case JsonLedgerClient.QueryResponse(results) =>
          val ctx = templateId.qualifiedName
          val ifaceType = Converter.toIfaceType(ctx, TTyCon(templateId)).right.get
          val parsedResults = results.map(r => {
            val payload = r.payload.convertTo[Value[AbsoluteContractId]](
              LfValueCodec.apiValueJsonReader(ifaceType, damlLfTypeLookup(_)))
            val cid = ContractIdString.assertFromString(r.contractId)
            ScriptLedgerClient.ActiveContract(templateId, AbsoluteContractId(cid), payload)
          })
          parsedResults
      }
  }
  override def submit(
      applicationId: ApplicationId,
      party: SParty,
      commands: List[ScriptLedgerClient.Command])(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    commands match {
      case Nil => Future { Right(List()) }
      case command :: Nil =>
        command match {
          case ScriptLedgerClient.CreateCommand(tplId, argument) =>
            create(tplId, argument).map(r => Right(List(r)))

          case ScriptLedgerClient.ExerciseCommand(tplId, cid, choice, argument) =>
            exercise(tplId, cid, choice, argument).map(r => Right(List(r)))

          case ScriptLedgerClient.ExerciseByKeyCommand(tplId, key, choice, argument) =>
            exerciseByKey(tplId, key, choice, argument).map(r => Right(List(r)))
          case ScriptLedgerClient.CreateAndExerciseCommand(tplId, template, choice, argument) =>
            createAndExercise(tplId, template, choice, argument)
        }
      case _ =>
        throw new RuntimeException(
          "Multi-command submissions are not supported by the HTTP JSON API.")
    }
  }
  override def allocateParty(partyIdHint: String, displayName: String)(
      implicit ec: ExecutionContext,
      mat: Materializer) = {
    throw new RuntimeException("allocateParty is not supported by the JSON API.")
  }

  private def create(
      tplId: Identifier,
      argument: Value[AbsoluteContractId]): Future[ScriptLedgerClient.CreateResult] = {
    val ctx = tplId.qualifiedName
    val ifaceType = Converter.toIfaceType(ctx, TTyCon(tplId)).right.get
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(uri.path./("v1")./("create")),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        JsonLedgerClient.CreateArgs(tplId, jsonArgument).toJson.prettyPrint),
      headers = List(Authorization(OAuth2BearerToken(token)))
    )
    Http()
      .singleRequest(req)
      .flatMap { resp =>
        Unmarshal(resp.entity).to[JsonLedgerClient.CreateResponse]
      }
      .map {
        case JsonLedgerClient.CreateResponse(cid) =>
          ScriptLedgerClient.CreateResult(
            AbsoluteContractId(ContractIdString.assertFromString(cid)))
      }
  }

  private def exercise(
      tplId: Identifier,
      contractId: AbsoluteContractId,
      choice: String,
      argument: Value[AbsoluteContractId]): Future[ScriptLedgerClient.ExerciseResult] = {
    val ctx = tplId.qualifiedName
    val choiceDef = envIface
      .typeDecls(tplId)
      .asInstanceOf[InterfaceType.Template]
      .template
      .choices(Name.assertFromString(choice))
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(uri.path./("v1")./("exercise")),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        JsonLedgerClient.ExerciseArgs(tplId, contractId, choice, jsonArgument).toJson.prettyPrint),
      headers = List(Authorization(OAuth2BearerToken(token)))
    )
    Http()
      .singleRequest(req)
      .flatMap { resp =>
        Unmarshal(resp.entity).to[JsonLedgerClient.ExerciseResponse]
      }
      .map {
        case JsonLedgerClient.ExerciseResponse(result) =>
          ScriptLedgerClient.ExerciseResult(
            tplId,
            choice,
            result.convertTo[Value[AbsoluteContractId]](
              LfValueCodec.apiValueJsonReader(choiceDef.returnType, damlLfTypeLookup(_))))
      }
  }

  private def exerciseByKey(
      tplId: Identifier,
      key: Value[AbsoluteContractId],
      choice: String,
      argument: Value[AbsoluteContractId]): Future[ScriptLedgerClient.ExerciseResult] = {
    val ctx = tplId.qualifiedName
    val choiceDef = envIface
      .typeDecls(tplId)
      .asInstanceOf[InterfaceType.Template]
      .template
      .choices(Name.assertFromString(choice))
    val jsonKey = LfValueCodec.apiValueToJsValue(key)
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(uri.path./("v1")./("exercise")),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        JsonLedgerClient
          .ExerciseByKeyArgs(tplId, jsonKey, choice, jsonArgument)
          .toJson
          .prettyPrint),
      headers = List(Authorization(OAuth2BearerToken(token)))
    )
    Http()
      .singleRequest(req)
      .flatMap { resp =>
        Unmarshal(resp.entity).to[JsonLedgerClient.ExerciseResponse]
      }
      .map {
        case JsonLedgerClient.ExerciseResponse(result) =>
          ScriptLedgerClient.ExerciseResult(
            tplId,
            choice,
            result.convertTo[Value[AbsoluteContractId]](
              LfValueCodec.apiValueJsonReader(choiceDef.returnType, damlLfTypeLookup(_))))
      }
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
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(uri.path./("v1")./("create-and-exercise")),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        JsonLedgerClient
          .CreateAndExerciseArgs(tplId, jsonTemplate, choice, jsonArgument)
          .toJson
          .prettyPrint),
      headers = List(Authorization(OAuth2BearerToken(token)))
    )
    Http()
      .singleRequest(req)
      .flatMap { resp =>
        Unmarshal(resp.entity).to[JsonLedgerClient.CreateAndExerciseResponse]
      }
      .map {
        case JsonLedgerClient.CreateAndExerciseResponse(cid, result) =>
          Right(
            List(
              ScriptLedgerClient.CreateResult(AbsoluteContractId(
                ContractIdString.assertFromString(cid))): ScriptLedgerClient.CommandResult,
              ScriptLedgerClient.ExerciseResult(
                tplId,
                choice,
                result.convertTo[Value[AbsoluteContractId]](
                  LfValueCodec.apiValueJsonReader(choiceDef.returnType, damlLfTypeLookup(_))))
            ))
      }
  }
}

object JsonLedgerClient {
  final case class QueryArgs(templateId: Identifier)
  final case class QueryResponse(result: List[ActiveContract])
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
  }
}
