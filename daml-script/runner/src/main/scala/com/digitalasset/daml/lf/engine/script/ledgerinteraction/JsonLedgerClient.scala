// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.ledgerinteraction

import java.time.Instant

import akka.util.ByteString
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.unmarshalling._
import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.jwt.JwtDecoder
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.auth.{
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  CustomDamlJWTPayload,
  StandardJWTPayload,
}
import com.daml.ledger.api.domain.{ObjectMeta, PartyDetails, User, UserRight}
import com.daml.lf.command
import com.daml.lf.data.Ref._
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.engine.script.{Converter, LfValueCodec}
import com.daml.lf.language.Ast
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.SValue
import com.daml.lf.typesig.EnvironmentSignature
import com.daml.lf.typesig.PackageSignature.TypeDecl
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import io.grpc.{Status, StatusRuntimeException}
import scalaz.OneAnd._
import scalaz.std.set._
import scalaz.syntax.equal._
import scalaz.syntax.foldable._
import scalaz.{-\/, OneAnd, \/-}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

// Current limitations and issues when running Daml script over the JSON API:
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
    envIface: EnvironmentSignature,
    actorSystem: ActorSystem,
) extends ScriptLedgerClient {
  import JsonLedgerClient.JsonProtocol._
  import JsonLedgerClient._

  override val transport = "JSON API"

  private val decodedJwt = JwtDecoder.decode(token) match {
    case -\/(e) => throw new IllegalArgumentException(e.toString)
    case \/-(a) => a
  }
  private[script] val tokenPayload: AuthServiceJWTPayload =
    AuthServiceJWTCodec.readFromString(decodedJwt.payload) match {
      case Failure(e) => throw e
      case Success(s) => s
    }

  implicit val system = actorSystem
  implicit val executionContext = system.dispatcher

  private def damlLfTypeLookup(id: Identifier) =
    envIface.typeDecls.get(id).map(_.`type`)

  val applicationId: Option[String] =
    tokenPayload match {
      case t: CustomDamlJWTPayload => t.applicationId
      case t: StandardJWTPayload =>
        // For standard jwts, the JSON API uses the user id
        // as the application id on command submissions.
        Some(t.userId)
    }

  def request[A, B](path: Path, a: A)(implicit
      wa: JsonWriter[A],
      rb: JsonReader[B],
  ): Future[Response[B]] = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri = uri.withPath(path),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        a.toJson.compactPrint,
      ),
      headers = List(Authorization(OAuth2BearerToken(token.value))),
    )
    Http()
      .singleRequest(req)
      .flatMap(resp =>
        Unmarshal(resp.entity).to[Response[B]].recoverWith { case _ =>
          resp.entity.dataBytes
            .runFold(ByteString.empty)((b, a) => b ++ a)
            .map(_.utf8String)
            .map(body => NonJsonErrorResponse(status = resp.status, body = body))
        }
      )
  }

  def request[A](path: Path)(implicit ra: JsonReader[A]): Future[Response[A]] = {
    val req = HttpRequest(
      method = HttpMethods.GET,
      uri = uri.withPath(path),
      headers = List(Authorization(OAuth2BearerToken(token.value))),
    )
    Http().singleRequest(req).flatMap(resp => Unmarshal(resp.entity).to[Response[A]])
  }

  // Update a js object with the given key & value (if it is Some)
  private def updateJsObject[A: JsonWriter, B: JsonWriter](v: A, key: String, optValue: Option[B]) =
    v.toJson match {
      case JsObject(o) => JsObject(optValue.fold(o)(v => o + (key -> v.toJson)))
      case other => throw new IllegalArgumentException(s"Expected JsObject but got $other")
    }

  def queryRequestSuccess[A: JsonWriter, B: JsonReader](
      path: Path,
      a: A,
      parties: Option[QueryParties],
  ): Future[B] = {
    val args = updateJsObject(a, "readers", parties.map(_.readers.toSet))
    requestSuccess[JsObject, B](path, args)
  }

  def requestSuccess[A, B](path: Path, a: A)(implicit
      wa: JsonWriter[A],
      rb: JsonReader[B],
  ): Future[B] =
    request[A, B](path, a).flatMap {
      case ErrorResponse(errors, status) =>
        Future.failed(FailedJsonApiRequest(path, Some(a.toJson), status, errors))
      case NonJsonErrorResponse(status, body) =>
        Future.failed(FailedJsonApiRequest(path, Some(a.toJson), status, List(body)))
      case SuccessResponse(result, _) => Future.successful(result)
    }

  def requestSuccess[A](path: Path)(implicit rb: JsonReader[A]): Future[A] =
    request[A](path).flatMap {
      case ErrorResponse(errors, status) =>
        Future.failed(FailedJsonApiRequest(path, None, status, errors))
      case NonJsonErrorResponse(status, body) =>
        Future.failed(FailedJsonApiRequest(path, None, status, List(body)))
      case SuccessResponse(result, _) => Future.successful(result)
    }

  override def query(parties: OneAnd[Set, Ref.Party], templateId: Identifier)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ) = {
    for {
      parties <- validateTokenParties(parties, "query")
      queryResponse <- queryRequestSuccess[QueryArgs, QueryResponse](
        uri.path./("v1")./("query"),
        QueryArgs(templateId),
        parties,
      )
    } yield {
      val ctx = templateId.qualifiedName
      val ifaceType = Converter.toIfaceType(ctx, TTyCon(templateId)).toOption.get
      val parsedResults = queryResponse.results.map(r => {
        val payload = r.payload.convertTo[Value](
          LfValueCodec.apiValueJsonReader(ifaceType, damlLfTypeLookup(_))
        )
        val cid = ContractId.assertFromString(r.contractId)
        ScriptLedgerClient.ActiveContract(templateId, cid, payload)
      })
      parsedResults
    }
  }

  override def queryContractId(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      cid: ContractId,
  )(implicit ec: ExecutionContext, mat: Materializer) = {
    for {
      parties <- validateTokenParties(parties, "queryContractId")
      fetchResponse <- queryRequestSuccess[FetchArgs, FetchResponse](
        uri.path./("v1")./("fetch"),
        FetchArgs(cid),
        parties,
      )
    } yield {
      val ctx = templateId.qualifiedName
      val ifaceType = Converter.toIfaceType(ctx, TTyCon(templateId)).toOption.get
      fetchResponse.result.map(r => {
        val payload = r.payload.convertTo[Value](
          LfValueCodec.apiValueJsonReader(ifaceType, damlLfTypeLookup(_))
        )
        val cid = ContractId.assertFromString(r.contractId)
        ScriptLedgerClient.ActiveContract(templateId, cid, payload)
      })
    }
  }

  override def queryInterface(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
      viewType: Ast.Type,
  )(implicit ec: ExecutionContext, mat: Materializer): Future[Seq[(ContractId, Option[Value])]] = {
    for {
      parties <- validateTokenParties(parties, "queryinterface")
      queryResponse <- queryRequestSuccess[QueryArgs, QueryResponse](
        uri.path./("v1")./("query"),
        QueryArgs(interfaceId),
        parties,
      )
    } yield {
      val ctx = interfaceId.qualifiedName
      val ifaceType = Converter.toIfaceType(ctx, viewType).toOption.get
      val parsedResults = queryResponse.results.map(r => {
        val payload = r.payload.convertTo[Value](
          LfValueCodec.apiValueJsonReader(ifaceType, damlLfTypeLookup(_))
        )
        val cid = ContractId.assertFromString(r.contractId)
        // TODO https://github.com/digital-asset/daml/issues/14830
        // contracts with failed-views are not returned over the Json API
        (cid, Some(payload))
      })
      parsedResults
    }
  }

  override def queryInterfaceContractId(
      parties: OneAnd[Set, Ref.Party],
      interfaceId: Identifier,
      viewType: Ast.Type,
      cid: ContractId,
  )(implicit ec: ExecutionContext, mat: Materializer): Future[Option[Value]] = {
    // Unfortunately, queryInterfaceContractId is linear in the ACS, since it must use the "query"
    // interface, unlike queryContractId which makes use of the "fetch" interface.
    for {
      parties <- validateTokenParties(parties, "queryinterfaceContractId")
      queryResponse <- queryRequestSuccess[QueryArgs, QueryResponse](
        uri.path./("v1")./("query"),
        QueryArgs(interfaceId),
        parties,
      )
    } yield {
      val ctx = interfaceId.qualifiedName
      val ifaceType = Converter.toIfaceType(ctx, viewType).toOption.get
      queryResponse.results.collectFirst(Function.unlift { r =>
        if (ContractId.assertFromString(r.contractId) != cid) None
        else {
          val payload = r.payload.convertTo[Value](
            LfValueCodec.apiValueJsonReader(ifaceType, damlLfTypeLookup(_))
          )
          Some(payload)
        }
      })
    }
  }

  override def queryContractKey(
      parties: OneAnd[Set, Ref.Party],
      templateId: Identifier,
      key: SValue,
      translateKey: (Identifier, Value) => Either[String, SValue],
  )(implicit ec: ExecutionContext, mat: Materializer) = {
    for {
      parties <- validateTokenParties(parties, "queryContractKey")
      fetchResponse <- queryRequestSuccess[FetchKeyArgs, FetchResponse](
        uri.path./("v1")./("fetch"),
        FetchKeyArgs(templateId, key.toUnnormalizedValue),
        parties,
      )
    } yield {
      val ctx = templateId.qualifiedName
      val ifaceType = Converter.toIfaceType(ctx, TTyCon(templateId)).toOption.get
      fetchResponse.result.map(r => {
        val payload = r.payload.convertTo[Value](
          LfValueCodec.apiValueJsonReader(ifaceType, damlLfTypeLookup(_))
        )
        val cid = ContractId.assertFromString(r.contractId)
        ScriptLedgerClient.ActiveContract(templateId, cid, payload)
      })
    }
  }
  override def submit(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[Either[StatusRuntimeException, Seq[ScriptLedgerClient.CommandResult]]] = {
    for {
      partySets <- validateSubmitParties(actAs, readAs)

      result <- commands match {
        case Nil => Future { Right(List()) }
        case cmd :: Nil =>
          cmd match {
            case command.CreateCommand(tplId, argument) =>
              create(tplId, argument, partySets)
            case command.ExerciseCommand(typeId, cid, choice, argument) =>
              exercise(typeId, cid, choice, argument, partySets)
            case command.ExerciseByKeyCommand(tplId, key, choice, argument) =>
              exerciseByKey(tplId, key, choice, argument, partySets)
            case command.CreateAndExerciseCommand(tplId, template, choice, argument) =>
              createAndExercise(tplId, template, choice, argument, partySets)
          }
        case _ =>
          Future.failed(
            new RuntimeException(
              "Multi-command submissions are not supported by the HTTP JSON API."
            )
          )
      }
    } yield result
  }
  override def submitMustFail(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit ec: ExecutionContext, mat: Materializer) = {
    submit(actAs, readAs, commands, optLocation).map {
      case Right(_) => Left(())
      case Left(_) => Right(())
    }
  }

  override def submitTree(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      commands: List[command.ApiCommand],
      optLocation: Option[Location],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Future[ScriptLedgerClient.TransactionTree] = {
    Future.failed(
      new RuntimeException(
        "submitTree is not supported when running Daml Script over the JSON API."
      )
    )
  }

  override def allocateParty(partyIdHint: String, displayName: String)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ) = {
    for {
      response <- requestSuccess[AllocatePartyArgs, AllocatePartyResponse](
        uri.path./("v1")./("parties")./("allocate"),
        AllocatePartyArgs(partyIdHint, displayName),
      )
    } yield {
      response.identifier
    }
  }

  override def listKnownParties()(implicit ec: ExecutionContext, mat: Materializer) = {
    requestSuccess[List[PartyDetails]](uri.path./("v1")./("parties"))
  }

  override def getStaticTime()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Time.Timestamp] = {
    // There is no time service in the JSON API so we default to the Unix epoch.
    Future { Time.Timestamp.assertFromInstant(Instant.EPOCH) }
  }

  override def setStaticTime(time: Time.Timestamp)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = {
    // No time service in the JSON API
    Future.failed(
      new RuntimeException("setTime is not supported when running Daml Script over the JSON API.")
    )
  }

  private def validateTokenParties(
      parties: OneAnd[Set, Ref.Party],
      what: String,
  ): Future[Option[QueryParties]] =
    JsonLedgerClient
      .validateTokenParties(parties, what, tokenPayload)
      .fold(s => Future.failed(new RuntimeException(s)), Future.successful(_))

  private def validateSubmitParties(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
  ): Future[Option[SubmitParties]] =
    JsonLedgerClient
      .validateSubmitParties(actAs, readAs, tokenPayload)
      .fold(s => Future.failed(new RuntimeException(s)), Future.successful(_))

  private def create(
      tplId: Identifier,
      argument: Value,
      partySets: Option[SubmitParties],
  ): Future[Either[StatusRuntimeException, List[ScriptLedgerClient.CreateResult]]] = {
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    commandRequest[CreateArgs, CreateResponse]("create", CreateArgs(tplId, jsonArgument), partySets)
      .map(_.map { case CreateResponse(cid) =>
        List(ScriptLedgerClient.CreateResult(ContractId.assertFromString(cid)))
      })
  }

  private def exercise(
      tplId: Identifier,
      contractId: ContractId,
      choice: ChoiceName,
      argument: Value,
      partySets: Option[SubmitParties],
  ): Future[Either[StatusRuntimeException, List[ScriptLedgerClient.ExerciseResult]]] = {
    val choiceDef = lookupChoice(tplId, choice)
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    commandRequest[ExerciseArgs, ExerciseResponse](
      "exercise",
      ExerciseArgs(tplId, contractId, choice, jsonArgument),
      partySets,
    )
      .map(_.map { case ExerciseResponse(result) =>
        List(
          ScriptLedgerClient.ExerciseResult(
            tplId,
            None,
            choice,
            result.convertTo[Value](
              LfValueCodec.apiValueJsonReader(choiceDef.returnType, damlLfTypeLookup(_))
            ),
          )
        )
      })
  }

  private def exerciseByKey(
      tplId: Identifier,
      key: Value,
      choice: ChoiceName,
      argument: Value,
      partySets: Option[SubmitParties],
  ): Future[Either[StatusRuntimeException, List[ScriptLedgerClient.ExerciseResult]]] = {
    val choiceDef = lookupChoice(tplId, choice)
    val jsonKey = LfValueCodec.apiValueToJsValue(key)
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    commandRequest[ExerciseByKeyArgs, ExerciseResponse](
      "exercise",
      ExerciseByKeyArgs(tplId, jsonKey, choice, jsonArgument),
      partySets,
    ).map(_.map { case ExerciseResponse(result) =>
      List(
        ScriptLedgerClient.ExerciseResult(
          tplId,
          None,
          choice,
          result.convertTo[Value](
            LfValueCodec.apiValueJsonReader(choiceDef.returnType, damlLfTypeLookup(_))
          ),
        )
      )
    })
  }

  private def createAndExercise(
      tplId: Identifier,
      template: Value,
      choice: ChoiceName,
      argument: Value,
      partySets: Option[SubmitParties],
  ): Future[Either[StatusRuntimeException, List[ScriptLedgerClient.CommandResult]]] = {
    val choiceDef = lookupChoice(tplId, choice)
    val jsonTemplate = LfValueCodec.apiValueToJsValue(template)
    val jsonArgument = LfValueCodec.apiValueToJsValue(argument)
    commandRequest[CreateAndExerciseArgs, CreateAndExerciseResponse](
      "create-and-exercise",
      CreateAndExerciseArgs(tplId, jsonTemplate, choice, jsonArgument),
      partySets,
    )
      .map(_.map { case CreateAndExerciseResponse(cid, result) =>
        List(
          ScriptLedgerClient
            .CreateResult(ContractId.assertFromString(cid)): ScriptLedgerClient.CommandResult,
          ScriptLedgerClient.ExerciseResult(
            tplId,
            None,
            choice,
            result.convertTo[Value](
              LfValueCodec.apiValueJsonReader(choiceDef.returnType, damlLfTypeLookup(_))
            ),
          ),
        )
      })
  }

  // TODO (#13973) this is not enough data to pick an interface choice
  private[this] def lookupChoice(tplId: Identifier, choice: ChoiceName) =
    envIface
      .typeDecls(tplId)
      .asInstanceOf[TypeDecl.Template]
      .template
      .tChoices
      .assumeNoOverloadedChoices(githubIssue = 13973)(choice)

  private[this] val SubmissionFailures: Set[StatusCode] = {
    import StatusCodes._
    Set(InternalServerError, BadRequest, Conflict, NotFound)
  }

  def commandRequest[In, Out](endpoint: String, argument: In, partySets: Option[SubmitParties])(
      implicit
      argumentWriter: JsonWriter[In],
      outputReader: RootJsonReader[Out],
  ): Future[Either[StatusRuntimeException, Out]] = {
    val argumentWithPartySets = updateJsObject(argument, "meta", partySets)
    request[JsObject, Out](uri.path./("v1")./(endpoint), argumentWithPartySets).flatMap {
      case ErrorResponse(errors, status) if SubmissionFailures(status) =>
        // TODO (MK) Using a grpc exception here doesn’t make that much sense.
        // We should refactor this to provide something more general.
        Future.successful(
          Left(new StatusRuntimeException(Status.UNKNOWN.withDescription(errors.toString)))
        )
      case ErrorResponse(errors, status) =>
        // XXX SC JSON API doesn't distinguish between
        // 400s that mean something like invalid JSON or “cannot resolve template ID”
        // and those that mean a submission error or assertion failure.
        // Ideally, the former would go through this path rather than be treated
        // as `submitMustFail` success
        Future.failed(
          new FailedJsonApiRequest(
            uri.path./("v1")./(endpoint),
            Some(argumentWithPartySets),
            status,
            errors,
          )
        )
      case NonJsonErrorResponse(status, body) =>
        Future.failed(
          new FailedJsonApiRequest(
            uri.path./("v1")./(endpoint),
            Some(argumentWithPartySets),
            status,
            List(body),
          )
        )
      case SuccessResponse(result, _) => Future.successful(Right(result))
    }
  }

  def recoverNotFound[A](e: Future[A]): Future[Option[A]] = {
    e.map(Some(_)).recover { case FailedJsonApiRequest(_, _, StatusCodes.NotFound, _) =>
      None
    }
  }
  def recoverAlreadyExists[A](e: Future[A]): Future[Option[A]] = {
    e.map(Some(_)).recover { case FailedJsonApiRequest(_, _, StatusCodes.Conflict, _) =>
      None
    }
  }

  override def createUser(
      user: User,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[Unit]] = {
    recoverAlreadyExists {
      requestSuccess[CreateUserRequest, ObjectResponse](
        uri.path./("v1")./("user")./("create"),
        CreateUserRequest(
          userId = user.id,
          primaryParty = user.primaryParty,
          rights,
          isAdmin = false,
        ),
      ).map(_ => ())
    }
  }

  override def getUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[User]] =
    recoverNotFound {
      requestSuccess[UserIdRequest, User](
        uri.path./("v1")./("user"),
        UserIdRequest(id),
      )
    }

  override def deleteUser(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[Unit]] =
    recoverNotFound {
      requestSuccess[UserIdRequest, ObjectResponse](
        uri.path./("v1")./("user")./("delete"),
        UserIdRequest(id),
      ).map(_ => ())
    }

  override def listAllUsers()(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[List[User]] = {
    requestSuccess[List[User]](uri.path./("v1")./("users"))
  }

  override def grantUserRights(
      id: UserId,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    recoverNotFound {
      requestSuccess[UserIdAndRightsRequest, List[UserRight]](
        uri.path./("v1")./("user")./("rights")./("grant"),
        UserIdAndRightsRequest(id, rights),
      )
    }

  override def revokeUserRights(
      id: UserId,
      rights: List[UserRight],
  )(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    recoverNotFound {
      requestSuccess[UserIdAndRightsRequest, List[UserRight]](
        uri.path./("v1")./("user")./("rights")./("revoke"),
        UserIdAndRightsRequest(id, rights),
      )
    }

  override def listUserRights(id: UserId)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Option[List[UserRight]]] =
    recoverNotFound {
      requestSuccess[UserIdRequest, List[UserRight]](
        uri.path./("v1")./("user")./("rights"),
        UserIdRequest(id),
      )
    }
}

object JsonLedgerClient {

  final case class CreateUserRequest(
      userId: String,
      primaryParty: Option[String],
      rights: List[UserRight],
      isAdmin: Boolean,
  )

  final case class UserIdRequest(userId: UserId)

  final case class UserIdAndRightsRequest(
      userId: UserId,
      rights: List[UserRight],
  )

  case class FailedJsonApiRequest(
      path: Path,
      reqBody: Option[JsValue],
      respStatus: StatusCode,
      errors: List[String],
  ) extends RuntimeException(
        s"Request to $path with ${reqBody.map(_.compactPrint)} failed with status $respStatus: $errors"
      )

  // Explicit party specifications for command submissions
  final case class SubmitParties(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
  )

  // Expect party specifications for queries
  final case class QueryParties(
      readers: OneAnd[Set, Ref.Party]
  )

  // Check that the parties in the token provide read claims for the given parties
  // and return explicit party specifications if required.
  def validateTokenParties(
      parties: OneAnd[Set, Ref.Party],
      what: String,
      tokenPayload: AuthServiceJWTPayload,
  ): Either[String, Option[QueryParties]] =
    tokenPayload match {
      case tokenPayload: CustomDamlJWTPayload =>
        val tokenParties = tokenPayload.readAs.toSet union tokenPayload.actAs.toSet
        val partiesSet = parties.toSet.toSet[String]
        val missingParties = partiesSet diff tokenParties
        // First check is just for a nicer error message and would be covered by the second
        if (tokenParties.isEmpty) {
          Left(
            s"Tried to $what as ${parties.toList.mkString(" ")} but token contains no parties."
          )
        } else if (missingParties.nonEmpty) {
          Left(s"Tried to $what as [${parties.toList
              .mkString(", ")}] but token provides claims for [${tokenParties
              .mkString(", ")}]. Missing claims: [${missingParties.mkString(", ")}]")
        } else {
          import scalaz.std.string._
          if (partiesSet === tokenParties) {
            // For backwards-compatibility we only set the party set flags when needed
            Right(None)
          } else {
            Right(Some(QueryParties(parties)))
          }
        }
      case _: StandardJWTPayload =>
        // A JSON API that understands standard JWTs also understands explicit party
        // specifications so rather than validating this client side, we just always set
        // the explicit party specification and leave it to the JSON API to validate this.
        Right(Some(QueryParties(parties)))
    }

  // Validate that the token has the required claims and return
  // SubmitParties we need to pass to the JSON API
  // if the token has more claims than we need.
  def validateSubmitParties(
      actAs: OneAnd[Set, Ref.Party],
      readAs: Set[Ref.Party],
      tokenPayload: AuthServiceJWTPayload,
  ): Either[String, Option[SubmitParties]] = {
    tokenPayload match {
      case tokenPayload: CustomDamlJWTPayload =>
        val actAsSet = actAs.toList.toSet[String]
        val readAsSet = readAs.toSet[String]
        val tokenActAs = tokenPayload.actAs.toSet
        val tokenReadAs = tokenPayload.readAs.toSet
        val missingActAs = actAs.toSet.toSet[String] diff tokenActAs
        val missingReadAs = readAs.toSet[String] diff (tokenReadAs union tokenActAs)
        if (tokenPayload.actAs.isEmpty) {
          Left(
            s"Tried to submit a command with actAs = [${actAs.toList.mkString(", ")}] but token contains no actAs parties."
          )

        } else if (missingActAs.nonEmpty) {
          Left(
            s"Tried to submit a command with actAs = [${actAs.toList.mkString(", ")}] but token provides claims for actAs = [${tokenPayload.actAs
                .mkString(", ")}]. Missing claims: [${missingActAs.mkString(", ")}]"
          )
        } else if (missingReadAs.nonEmpty) {
          Left(
            s"Tried to submit a command with readAs = [${readAs.mkString(", ")}] but token provides claims for readAs = [${tokenPayload.readAs
                .mkString(", ")}]. Missing claims: [${missingReadAs.mkString(", ")}]"
          )
        } else {
          import scalaz.std.string._
          val onlyReadAs = readAsSet diff actAsSet
          val tokenOnlyReadAs = tokenReadAs diff tokenActAs
          if (onlyReadAs === tokenOnlyReadAs && actAsSet === tokenActAs) {
            // For backwards-compatibility we only set the party set flags when needed
            Right(None)
          } else {
            Right(Some(SubmitParties(actAs, readAs)))
          }
        }
      case _: StandardJWTPayload =>
        // A JSON API that understands standard JWTs also understands explicit party
        // specifications so rather than validating this client side, we just always set
        // the explicit party specification and leave it to the JSON API to validate this.
        Right(Some(SubmitParties(actAs, readAs)))
    }
  }

  sealed trait Response[A] {
    def status: StatusCode
  }
  final case class ErrorResponse[A](errors: List[String], status: StatusCode) extends Response[A]
  final case class NonJsonErrorResponse[A](status: StatusCode, body: String) extends Response[A]
  final case class SuccessResponse[A](result: A, status: StatusCode) extends Response[A]

  final case class QueryArgs(templateId: Identifier)
  final case class QueryResponse(results: List[ActiveContract])
  final case class ActiveContract(contractId: String, payload: JsValue)
  final case class FetchArgs(contractId: ContractId)
  final case class FetchKeyArgs(templateId: Identifier, key: Value)
  final case class FetchResponse(result: Option[ActiveContract])

  final case class CreateArgs(templateId: Identifier, payload: JsValue)
  final case class CreateResponse(contractId: String)

  final case class ExerciseArgs(
      templateId: Identifier,
      contractId: ContractId,
      choice: ChoiceName,
      argument: JsValue,
  )
  final case class ExerciseResponse(result: JsValue)

  final case class ExerciseByKeyArgs(
      templateId: Identifier,
      key: JsValue,
      choice: ChoiceName,
      argument: JsValue,
  )

  final case class CreateAndExerciseArgs(
      templateId: Identifier,
      payload: JsValue,
      choice: ChoiceName,
      argument: JsValue,
  )
  final case class CreateAndExerciseResponse(contractId: String, result: JsValue)

  final case class AllocatePartyArgs(
      identifierHint: String,
      displayName: String,
  )
  final case class AllocatePartyResponse(identifier: Ref.Party)

// Any JS object, we validate that it’s an object to catch issues in our request but we ignore all fields
// for forwards compatibility.
  final case class ObjectResponse()

  object JsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
    implicit def optionReader[A: JsonReader]: JsonReader[Option[A]] =
      v =>
        v match {
          case JsNull => None
          case _ => Some(v.convertTo[A])
        }
    implicit def listReader[A: JsonReader]: JsonReader[List[A]] =
      v =>
        v match {
          case JsArray(xs) => xs.toList.map(_.convertTo[A])
          case _ => deserializationError(s"Expected JsArray but got $v")
        }
    implicit def responseReader[A: JsonReader]: RootJsonReader[Response[A]] = v => {
      implicit val statusCodeReader: JsonReader[StatusCode] = v =>
        v match {
          case JsNumber(value) => StatusCode.int2StatusCode(value.toIntExact)
          case _ => deserializationError("Expected status code")
        }
      val obj = v.asJsObject
      (obj.fields.get("status"), obj.fields.get("errors"), obj.fields.get("result")) match {
        case (Some(status), Some(err), None) =>
          ErrorResponse(
            err.convertTo[List[String]](DefaultJsonProtocol.listFormat),
            status.convertTo[StatusCode],
          )
        case (Some(status), _, Some(res)) =>
          SuccessResponse(res.convertTo[A], status.convertTo[StatusCode])
        case _ => deserializationError("Expected status and either errors or result field")
      }
    }

    implicit val partyReader: JsonReader[Ref.Party] = v =>
      v match {
        case JsString(s) => Ref.Party.fromString(s).fold(deserializationError(_), identity)
        case _ => deserializationError(s"Expected Party but got $v")
      }
    implicit val partyDetailsReader: JsonReader[PartyDetails] = v => {
      val o = v.asJsObject
      (o.fields.get("identifier"), o.fields.get("displayName"), o.fields.get("isLocal")) match {
        case (Some(id), optName, Some(isLocal)) =>
          PartyDetails(
            id.convertTo[Party],
            optName.map(_.convertTo[String]),
            isLocal.convertTo[Boolean],
            ObjectMeta.empty,
          )
        case _ => deserializationError(s"Expected PartyDetails but got $v")
      }
    }

    implicit val choiceNameWriter: JsonWriter[ChoiceName] = choice => JsString(choice.toString)
    implicit val identifierWriter: JsonWriter[Identifier] = identifier =>
      JsString(
        identifier.packageId + ":" + identifier.qualifiedName.module.toString + ":" + identifier.qualifiedName.name.toString
      )

    implicit val queryWriter: JsonWriter[QueryArgs] = args =>
      JsObject("templateIds" -> JsArray(identifierWriter.write(args.templateId)))
    implicit val queryReader: RootJsonReader[QueryResponse] = v =>
      QueryResponse(v.convertTo[List[ActiveContract]])
    implicit val fetchWriter: JsonWriter[FetchArgs] = args =>
      JsObject("contractId" -> args.contractId.coid.toString.toJson)
    implicit val fetchKeyWriter: JsonWriter[FetchKeyArgs] = args =>
      JsObject(
        "templateId" -> args.templateId.toJson,
        "key" -> LfValueCodec.apiValueToJsValue(args.key),
      )
    implicit val fetchReader: RootJsonReader[FetchResponse] = v =>
      FetchResponse(v.convertTo[Option[ActiveContract]])

    implicit val activeContractReader: RootJsonReader[ActiveContract] = v => {
      v.asJsObject.getFields("contractId", "payload") match {
        case Seq(JsString(s), v) => ActiveContract(s, v)
        case _ => deserializationError(s"Could not parse ActiveContract: $v")
      }
    }

    implicit val partyFormat: JsonFormat[Ref.Party] = new JsonFormat[Ref.Party] {
      override def write(p: Ref.Party) = JsString(p)
      override def read(json: JsValue) = json match {
        case JsString(p) =>
          Party.fromString(p) match {
            case Left(err) => deserializationError(err)
            case Right(p) => p
          }
        case _ => deserializationError(s"Expected party but got $json")
      }
    }

    implicit val submitPartiesWriter: JsonWriter[SubmitParties] = parties =>
      JsObject("actAs" -> parties.actAs.toList.toJson, "readAs" -> parties.readAs.toJson)

    implicit val createWriter: JsonWriter[CreateArgs] = args =>
      JsObject("templateId" -> args.templateId.toJson, "payload" -> args.payload)
    implicit val createReader: RootJsonReader[CreateResponse] = v =>
      v.asJsObject.getFields("contractId") match {
        case Seq(JsString(cid)) => CreateResponse(cid)
        case _ => deserializationError(s"Could not parse CreateResponse: $v")
      }

    implicit val exerciseWriter: JsonWriter[ExerciseArgs] = args =>
      JsObject(
        "templateId" -> args.templateId.toJson,
        "contractId" -> args.contractId.coid.toString.toJson,
        "choice" -> args.choice.toJson,
        "argument" -> args.argument,
      )
    implicit val exerciseByKeyWriter: JsonWriter[ExerciseByKeyArgs] = args =>
      JsObject(
        "templateId" -> args.templateId.toJson,
        "key" -> args.key,
        "choice" -> args.choice.toJson,
        "argument" -> args.argument,
      )
    implicit val exerciseReader: RootJsonReader[ExerciseResponse] = v =>
      v.asJsObject.getFields("exerciseResult") match {
        case Seq(result) => ExerciseResponse(result)
        case _ => deserializationError(s"Could not parse ExerciseResponse: $v")
      }

    implicit val createAndExerciseWriter: JsonWriter[CreateAndExerciseArgs] = args =>
      JsObject(
        "templateId" -> args.templateId.toJson,
        "payload" -> args.payload,
        "choice" -> args.choice.toJson,
        "argument" -> args.argument,
      )
    implicit val createAndExerciseReader: RootJsonReader[CreateAndExerciseResponse] = v =>
      v.asJsObject.getFields("exerciseResult", "events") match {
        case Seq(result, events) =>
          events match {
            case JsArray(Seq(event, _*)) =>
              event.asJsObject.getFields("created") match {
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

    implicit val allocatePartyWriter: JsonFormat[AllocatePartyArgs] = jsonFormat2(AllocatePartyArgs)
    implicit val allocatePartyReader: RootJsonReader[AllocatePartyResponse] = v =>
      v.asJsObject.getFields("identifier") match {
        case Seq(id) => AllocatePartyResponse(id.convertTo[Party])
        case _ => deserializationError(s"Could not parse AllocatePartyResponse: $v")
      }

    implicit val userId: JsonFormat[UserId] = new JsonFormat[UserId] {
      override def write(id: UserId) = JsString(id)
      override def read(json: JsValue) = {
        json match {
          case JsString(s) => Ref.UserId.fromString(s).fold(deserializationError(_), identity)
          case _ => deserializationError(s"Expected UserId but got $json")
        }
      }
    }

    implicit val userReader: JsonReader[User] = json => {
      val o = json.asJsObject
      (o.fields.get("userId"), o.fields.get("primaryParty")) match {
        case (Some(id), primaryPartyOpt) =>
          User(
            id = id.convertTo[UserId],
            primaryParty = primaryPartyOpt.map(_.convertTo[Party]),
          )
        case _ => deserializationError(s"Expected User but got $json")
      }
    }

    implicit val listUserRight: JsonFormat[List[UserRight]] = new JsonFormat[List[UserRight]] {
      override def write(xs: List[UserRight]) = JsArray(xs.map(_.toJson).toVector)
      override def read(json: JsValue) = json match {
        case JsArray(elements) => elements.iterator.map(_.convertTo[UserRight]).toList
        case _ => deserializationError(s"must be a list, but got $json")
      }
    }

    implicit val userRightFormat: JsonFormat[UserRight] = new JsonFormat[UserRight] {
      override def write(x: UserRight) = x match {
        case UserRight.CanReadAs(party) =>
          JsObject("type" -> JsString("CanReadAs"), "party" -> JsString(party))
        case UserRight.CanActAs(party) =>
          JsObject("type" -> JsString("CanActAs"), "party" -> JsString(party))
        case UserRight.ParticipantAdmin =>
          JsObject("type" -> JsString("ParticipantAdmin"))
      }
      override def read(json: JsValue) = {
        val obj = json.asJsObject
        obj.fields.get("type") match {
          case Some(JsString("ParticipantAdmin")) => UserRight.ParticipantAdmin
          case Some(JsString("CanReadAs")) =>
            obj.fields.get("party") match {
              case None => deserializationError("UserRight.CanReadAs")
              case Some(party) => UserRight.CanReadAs(party.convertTo[Party])
            }
          case Some(JsString("CanActAs")) =>
            obj.fields.get("party") match {
              case None => deserializationError("UserRight.CanActAs")
              case Some(party) => UserRight.CanActAs(party.convertTo[Party])
            }
          case _ =>
            deserializationError("UserRight")
        }
      }
    }

    implicit val createUserFormat: JsonFormat[CreateUserRequest] = jsonFormat4(CreateUserRequest)

    implicit val userIdRequestFormat: JsonFormat[UserIdRequest] = jsonFormat1(UserIdRequest)

    implicit val userIdAndRightsFormat: JsonFormat[UserIdAndRightsRequest] = jsonFormat2(
      UserIdAndRightsRequest
    )

    implicit val objectResponse: RootJsonReader[ObjectResponse] = v => {
      v match {
        case JsObject(_) => ObjectResponse()
        case _ => deserializationError("ObjectResponse")
      }
    }

  }
}
