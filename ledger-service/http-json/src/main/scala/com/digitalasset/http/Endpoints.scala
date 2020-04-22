// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString
import com.daml.lf
import com.daml.http.ContractsService.SearchResult
import com.daml.http.EndpointsCompanion._
import com.daml.http.Statement.discard
import com.daml.http.domain.JwtPayload
import com.daml.http.json._
import com.daml.http.util.Collections.toNonEmptySet
import com.daml.http.util.FutureUtil.{either, eitherT}
import com.daml.http.util.ProtobufByteStrings
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.{v1 => lav1}
import com.daml.util.ExceptionOps._
import com.typesafe.scalalogging.StrictLogging
import scalaz.std.scalaFuture._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, NonEmptyList, Show, \/, \/-}
import spray.json._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class Endpoints(
    ledgerId: lar.LedgerId,
    decodeJwt: EndpointsCompanion.ValidateJwt,
    commandService: CommandService,
    contractsService: ContractsService,
    partiesService: PartiesService,
    packageManagementService: PackageManagementService,
    encoder: DomainJsonEncoder,
    decoder: DomainJsonDecoder,
    maxTimeToCollectRequest: FiniteDuration = FiniteDuration(5, "seconds"))(
    implicit ec: ExecutionContext,
    mat: Materializer)
    extends StrictLogging {

  import Endpoints._
  import encoder.implicits._
  import json.JsonProtocol._
  import util.ErrorOps._
  import Uri.Path._

  lazy val all: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case req @ HttpRequest(POST, Uri.Path("/v1/create"), _, _, _) => httpResponse(create(req))
    case req @ HttpRequest(POST, Uri.Path("/v1/exercise"), _, _, _) => httpResponse(exercise(req))
    case req @ HttpRequest(POST, Uri.Path("/v1/create-and-exercise"), _, _, _) =>
      httpResponse(createAndExercise(req))
    case req @ HttpRequest(POST, Uri.Path("/v1/fetch"), _, _, _) => httpResponse(fetch(req))
    case req @ HttpRequest(GET, Uri.Path("/v1/query"), _, _, _) => httpResponse(retrieveAll(req))
    case req @ HttpRequest(POST, Uri.Path("/v1/query"), _, _, _) => httpResponse(query(req))
    case req @ HttpRequest(GET, Uri.Path("/v1/parties"), _, _, _) => httpResponse(allParties(req))
    case req @ HttpRequest(POST, Uri.Path("/v1/parties"), _, _, _) => httpResponse(parties(req))
    case req @ HttpRequest(POST, Uri.Path("/v1/parties/allocate"), _, _, _) =>
      httpResponse(allocateParty(req))
    case req @ HttpRequest(GET, Uri.Path("/v1/packages"), _, _, _) =>
      httpResponse(listPackages(req))
    // format: off
    case req @ HttpRequest(
          GET, Uri(_, _, Slash(Segment("v1", Slash(Segment("packages", Slash(Segment(packageId, Empty)))))), _, _),
          _, _, _) =>
      downloadPackage(req, packageId)
    // format: on
    case req @ HttpRequest(POST, Uri.Path("/v1/packages"), _, _, _) =>
      httpResponse(uploadDarFile(req))
  }

  def create(req: HttpRequest): ET[domain.SyncResponse[JsValue]] =
    for {
      t3 <- inputJsValAndJwtPayload(req): ET[(Jwt, JwtPayload, JsValue)]

      (jwt, jwtPayload, reqBody) = t3

      cmd <- either(
        decoder.decodeCreateCommand(reqBody).liftErr(InvalidUserInput)
      ): ET[domain.CreateCommand[ApiRecord]]

      ac <- eitherT(
        handleFutureEitherFailure(commandService.create(jwt, jwtPayload, cmd))
      ): ET[domain.ActiveContract[ApiValue]]

      jsVal <- either(SprayJson.encode1(ac).liftErr(ServerError)): ET[JsValue]

    } yield domain.OkResponse(jsVal)

  def exercise(req: HttpRequest): ET[domain.SyncResponse[JsValue]] =
    for {
      t3 <- inputJsValAndJwtPayload(req): ET[(Jwt, JwtPayload, JsValue)]

      (jwt, jwtPayload, reqBody) = t3

      cmd <- either(
        decoder.decodeExerciseCommand(reqBody).liftErr(InvalidUserInput)
      ): ET[domain.ExerciseCommand[LfValue, domain.ContractLocator[LfValue]]]

      resolvedRef <- eitherT(
        resolveReference(jwt, jwtPayload, cmd.reference)
      ): ET[domain.ResolvedContractRef[ApiValue]]

      apiArg <- either(lfValueToApiValue(cmd.argument)): ET[ApiValue]

      resolvedCmd = cmd.copy(argument = apiArg, reference = resolvedRef)

      resp <- eitherT(
        handleFutureEitherFailure(commandService.exercise(jwt, jwtPayload, resolvedCmd))
      ): ET[domain.ExerciseResponse[ApiValue]]

      jsVal <- either(SprayJson.encode1(resp).liftErr(ServerError)): ET[JsValue]

    } yield domain.OkResponse(jsVal)

  def createAndExercise(req: HttpRequest): ET[domain.SyncResponse[JsValue]] =
    for {
      t3 <- inputJsValAndJwtPayload(req): ET[(Jwt, JwtPayload, JsValue)]

      (jwt, jwtPayload, reqBody) = t3

      cmd <- either(
        decoder.decodeCreateAndExerciseCommand(reqBody).liftErr(InvalidUserInput)
      ): ET[domain.CreateAndExerciseCommand[ApiRecord, ApiValue]]

      resp <- eitherT(
        handleFutureEitherFailure(commandService.createAndExercise(jwt, jwtPayload, cmd))
      ): ET[domain.ExerciseResponse[ApiValue]]

      jsVal <- either(SprayJson.encode1(resp).liftErr(ServerError)): ET[JsValue]

    } yield domain.OkResponse(jsVal)

  def fetch(req: HttpRequest): ET[domain.SyncResponse[JsValue]] =
    for {
      input <- inputJsValAndJwtPayload(req): ET[(Jwt, JwtPayload, JsValue)]

      (jwt, jwtPayload, reqBody) = input

      _ = logger.debug(s"/v1/fetch reqBody: $reqBody")

      cl <- either(
        decoder.decodeContractLocator(reqBody).liftErr(InvalidUserInput)
      ): ET[domain.ContractLocator[LfValue]]

      _ = logger.debug(s"/v1/fetch cl: $cl")

      ac <- eitherT(
        handleFutureFailure(contractsService.lookup(jwt, jwtPayload, cl))
      ): ET[Option[domain.ActiveContract[LfValue]]]

      jsVal <- either(
        ac.cata(x => lfAcToJsValue(x), \/-(JsNull))
      ): ET[JsValue]

    } yield domain.OkResponse(jsVal)

  def retrieveAll(req: HttpRequest): Future[Error \/ SearchResult[Error \/ JsValue]] =
    inputAndJwtPayload(req).map {
      _.map {
        case (jwt, jwtPayload, _) =>
          val result: SearchResult[ContractsService.Error \/ domain.ActiveContract[LfValue]] =
            contractsService.retrieveAll(jwt, jwtPayload)

          domain.SyncResponse.covariant.map(result) { source =>
            source
              .via(handleSourceFailure)
              .map(_.flatMap(lfAcToJsValue)): Source[Error \/ JsValue, NotUsed]
          }
      }
    }

  def query(req: HttpRequest): Future[Error \/ SearchResult[Error \/ JsValue]] =
    inputAndJwtPayload(req).map {
      _.flatMap {
        case (jwt, jwtPayload, reqBody) =>
          SprayJson
            .decode[domain.GetActiveContractsRequest](reqBody)
            .liftErr(InvalidUserInput)
            .map { cmd =>
              val result: SearchResult[ContractsService.Error \/ domain.ActiveContract[JsValue]] =
                contractsService.search(jwt, jwtPayload, cmd)

              domain.SyncResponse.covariant.map(result) { source =>
                source
                  .via(handleSourceFailure)
                  .map(_.flatMap(toJsValue[domain.ActiveContract[JsValue]](_)))
              }
            }
      }
    }

  def allParties(req: HttpRequest): ET[domain.SyncResponse[List[domain.PartyDetails]]] =
    proxyWithoutCommand(partiesService.allParties)(req).map(domain.OkResponse(_))

  def parties(req: HttpRequest): ET[domain.SyncResponse[List[domain.PartyDetails]]] =
    proxyWithCommand[NonEmptyList[domain.Party], (Set[domain.PartyDetails], Set[domain.Party])](
      (jwt, cmd) => partiesService.parties(jwt, toNonEmptySet(cmd)))(req)
      .map(ps => partiesResponse(parties = ps._1.toList, unknownParties = ps._2.toList))

  def allocateParty(req: HttpRequest): ET[domain.SyncResponse[domain.PartyDetails]] =
    proxyWithCommand(partiesService.allocate)(req).map(domain.OkResponse(_))

  def listPackages(req: HttpRequest): ET[domain.SyncResponse[Seq[String]]] =
    proxyWithoutCommand(packageManagementService.listPackages)(req).map(domain.OkResponse(_))

  def downloadPackage(req: HttpRequest, packageId: String): Future[HttpResponse] = {
    val et: ET[admin.GetPackageResponse] =
      proxyWithoutCommand(jwt => packageManagementService.getPackage(jwt, packageId))(req)
    val fa: Future[Error \/ admin.GetPackageResponse] = et.run
    fa.map {
      case -\/(e) =>
        httpResponseError(e)
      case \/-(x) =>
        HttpResponse(
          entity = HttpEntity.apply(
            ContentTypes.`application/octet-stream`,
            ProtobufByteStrings.toSource(x.archivePayload))
        )
    }
  }

  def uploadDarFile(req: HttpRequest): ET[domain.SyncResponse[Unit]] =
    for {
      t2 <- either(inputSource(req)): ET[(Jwt, Source[ByteString, Any])]

      (jwt, source) = t2

      _ <- eitherT(
        handleFutureFailure(
          packageManagementService.uploadDarFile(jwt, source.mapMaterializedValue(_ => NotUsed))
        )): ET[Unit]

    } yield domain.OkResponse(())

  private def handleFutureEitherFailure[B](fa: Future[Error \/ B]): Future[Error \/ B] =
    fa.recover {
      case NonFatal(e) =>
        logger.error("Future failed", e)
        -\/(ServerError(e.description))
    }

  private def handleFutureEitherFailure[A: Show, B](fa: Future[A \/ B]): Future[ServerError \/ B] =
    fa.map(_.liftErr(ServerError)).recover {
      case NonFatal(e) =>
        logger.error("Future failed", e)
        -\/(ServerError(e.description))
    }

  private def handleFutureFailure[A](fa: Future[A]): Future[ServerError \/ A] =
    fa.map(a => \/-(a)).recover {
      case NonFatal(e) =>
        logger.error("Future failed", e)
        -\/(ServerError(e.description))
    }

  private def handleSourceFailure[E: Show, A]: Flow[E \/ A, ServerError \/ A, NotUsed] =
    Flow
      .fromFunction((_: E \/ A).liftErr(ServerError))
      .recover {
        case NonFatal(e) =>
          logger.error("Source failed", e)
          -\/(ServerError(e.description))
      }

  private def httpResponse(
      output: Future[Error \/ SearchResult[Error \/ JsValue]]): Future[HttpResponse] =
    output
      .map {
        case -\/(e) => httpResponseError(e)
        case \/-(searchResult) => httpResponse(searchResult)
      }
      .recover {
        case NonFatal(e) => httpResponseError(ServerError(e.description))
      }

  private def httpResponse(searchResult: SearchResult[Error \/ JsValue]): HttpResponse = {
    import json.JsonProtocol._

    val response: Source[ByteString, NotUsed] = searchResult match {
      case domain.OkResponse(result, warnings, _) =>
        val warningsJsVal: Option[JsValue] = warnings.map(SprayJson.encodeUnsafe(_))
        ResponseFormats.resultJsObject(result, warningsJsVal)
      case error: domain.ErrorResponse =>
        val jsVal: JsValue = SprayJson.encodeUnsafe(error)
        Source.single(ByteString(jsVal.compactPrint))
    }

    HttpResponse(
      status = StatusCodes.OK,
      entity = HttpEntity.CloseDelimited(ContentTypes.`application/json`, response)
    )
  }

  private def httpResponse[A: JsonWriter](
      result: ET[domain.SyncResponse[A]]
  ): Future[HttpResponse] = {
    val fa: Future[Error \/ (JsValue, StatusCode)] =
      result.flatMap { x =>
        either(SprayJson.encode1(x).map(y => (y, x.status)).liftErr(ServerError))
      }.run
    fa.map {
        case -\/(e) =>
          httpResponseError(e)
        case \/-((jsVal, status)) =>
          HttpResponse(
            entity = HttpEntity.Strict(ContentTypes.`application/json`, format(jsVal)),
            status = status)
      }
      .recover {
        case NonFatal(e) => httpResponseError(ServerError(e.description))
      }
  }

  private[http] def data(entity: RequestEntity): Future[String] =
    entity.toStrict(maxTimeToCollectRequest).map(_.data.utf8String)

  private[http] def input(req: HttpRequest): Future[Unauthorized \/ (Jwt, String)] = {
    findJwt(req) match {
      case e @ -\/(_) =>
        discard { req.entity.discardBytes(mat) }
        Future.successful(e)
      case \/-(j) =>
        data(req.entity).map(d => \/-((j, d)))
    }
  }

  private[http] def inputJsVal(req: HttpRequest): ET[(Jwt, JsValue)] =
    for {
      t2 <- eitherT(input(req)): ET[(Jwt, String)]
      jsVal <- either(SprayJson.parse(t2._2).liftErr(InvalidUserInput)): ET[JsValue]
    } yield (t2._1, jsVal)

  private[http] def withJwtPayload[A](fa: (Jwt, A)): Unauthorized \/ (Jwt, JwtPayload, A) =
    decodeAndParsePayload(fa._1, decodeJwt).map(t2 => (t2._1, t2._2, fa._2))

  private[http] def inputAndJwtPayload(
      req: HttpRequest
  ): Future[Unauthorized \/ (Jwt, JwtPayload, String)] =
    input(req).map(_.flatMap(withJwtPayload))

  private[http] def inputJsValAndJwtPayload(req: HttpRequest): ET[(Jwt, JwtPayload, JsValue)] =
    inputJsVal(req).flatMap(x => either(withJwtPayload(x)))

  private[http] def inputSource(
      req: HttpRequest
  ): Error \/ (Jwt, Source[ByteString, Any]) =
    findJwt(req) match {
      case e @ -\/(_) =>
        discard { req.entity.discardBytes(mat) }
        e
      case \/-(j) =>
        \/-((j, req.entity.dataBytes))
    }

  private[http] def findJwt(req: HttpRequest): Unauthorized \/ Jwt =
    req.headers
      .collectFirst {
        case Authorization(OAuth2BearerToken(token)) => Jwt(token)
      }
      .toRightDisjunction(Unauthorized("missing Authorization header with OAuth 2.0 Bearer Token"))

  private def resolveReference(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      reference: domain.ContractLocator[LfValue])
    : Future[Error \/ domain.ResolvedContractRef[ApiValue]] =
    contractsService
      .resolveContractReference(jwt, jwtPayload, reference)
      .map { o: Option[domain.ResolvedContractRef[LfValue]] =>
        val a: Error \/ domain.ResolvedContractRef[LfValue] =
          o.toRightDisjunction(InvalidUserInput(ErrorMessages.cannotResolveTemplateId(reference)))
        a.flatMap {
          case -\/((tpId, key)) => lfValueToApiValue(key).map(k => -\/((tpId, k)))
          case a @ \/-((_, _)) => \/-(a)
        }
      }

  private def proxyWithoutCommand[A](fn: Jwt => Future[A])(req: HttpRequest): ET[A] =
    for {
      t3 <- eitherT(input(req)): ET[(Jwt, _)]
      a <- eitherT(handleFutureFailure(fn(t3._1))): ET[A]
    } yield a

  private def proxyWithCommand[A: JsonReader, R](fn: (Jwt, A) => Future[Error \/ R])(
      req: HttpRequest): ET[R] =
    for {
      t2 <- inputJsVal(req): ET[(Jwt, JsValue)]
      (jwt, reqBody) = t2
      a <- either(SprayJson.decode[A](reqBody).liftErr(InvalidUserInput)): ET[A]
      b <- eitherT(handleFutureEitherFailure(fn(jwt, a))): ET[R]
    } yield b
}

object Endpoints {
  import json.JsonProtocol._
  import util.ErrorOps._

  private type ET[A] = EitherT[Future, Error, A]

  private type ApiRecord = lav1.value.Record
  private type ApiValue = lav1.value.Value

  private type LfValue = lf.value.Value[lf.value.Value.AbsoluteContractId]

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.fromTryCatchNonFatal(LfValueCodec.apiValueToJsValue(a)).liftErr(ServerError)

  private def lfValueToApiValue(a: LfValue): Error \/ ApiValue =
    JsValueToApiValueConverter.lfValueToApiValue(a).liftErr(ServerError)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def lfAcToJsValue(a: domain.ActiveContract[LfValue]): Error \/ JsValue = {
    for {
      b <- a.traverse(lfValueToJsValue): Error \/ domain.ActiveContract[JsValue]
      c <- toJsValue(b)
    } yield c
  }

  private def partiesResponse(
      parties: List[domain.PartyDetails],
      unknownParties: List[domain.Party]
  ): domain.SyncResponse[List[domain.PartyDetails]] = {

    val warnings: Option[domain.UnknownParties] =
      if (unknownParties.isEmpty) None
      else Some(domain.UnknownParties(unknownParties))

    domain.OkResponse(parties, warnings)
  }

  private def toJsValue[A: JsonWriter](a: A): Error \/ JsValue = {
    SprayJson.encode(a).liftErr(ServerError)
  }
}
