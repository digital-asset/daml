// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import com.digitalasset.daml.lf
import com.digitalasset.http.ContractsService.SearchResult
import com.digitalasset.http.EndpointsCompanion._
import com.digitalasset.http.Statement.discard
import com.digitalasset.http.domain.JwtPayload
import com.digitalasset.http.json._
import com.digitalasset.http.util.FutureUtil.{either, eitherT, rightT}
import com.digitalasset.http.util.{ApiValueToLfValueConverter, FutureUtil}
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import com.digitalasset.util.ExceptionOps._
import com.typesafe.scalalogging.StrictLogging
import scalaz.std.scalaFuture._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.std.option._
import scalaz.syntax.traverse._
import scalaz.syntax.apply._
import scalaz.syntax.bitraverse._
import scalaz.{-\/, Bitraverse, EitherT, Show, \/, \/-}
import spray.json._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

import scala.language.higherKinds

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class Endpoints(
    ledgerId: lar.LedgerId,
    decodeJwt: EndpointsCompanion.ValidateJwt,
    commandService: CommandService,
    contractsService: ContractsService,
    partiesService: PartiesService,
    encoder: DomainJsonEncoder,
    decoder: DomainJsonDecoder,
    maxTimeToCollectRequest: FiniteDuration = FiniteDuration(5, "seconds"))(
    implicit ec: ExecutionContext,
    mat: Materializer)
    extends StrictLogging {

  import Endpoints._
  import json.JsonProtocol._

  lazy val all: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case req @ HttpRequest(POST, Uri.Path("/v1/create"), _, _, _) => httpResponse(create(req))
    case req @ HttpRequest(POST, Uri.Path("/v1/exercise"), _, _, _) => httpResponse(exercise(req))
    case req @ HttpRequest(POST, Uri.Path("/v1/fetch"), _, _, _) => httpResponse(fetch(req))
    case req @ HttpRequest(GET, Uri.Path("/v1/query"), _, _, _) => httpResponse(retrieveAll(req))
    case req @ HttpRequest(POST, Uri.Path("/v1/query"), _, _, _) => httpResponse(query(req))
    case req @ HttpRequest(GET, Uri.Path("/v1/parties"), _, _, _) => httpResponse(parties(req))
  }

  def create(req: HttpRequest): ET[JsValue] =
    for {
      t3 <- FutureUtil.eitherT(input(req)): ET[(Jwt, JwtPayload, String)]

      (jwt, jwtPayload, reqBody) = t3

      cmd <- either(
        decoder
          .decodeR[domain.CreateCommand](reqBody)
          .leftMap(e => InvalidUserInput(e.shows))
      ): ET[domain.CreateCommand[lav1.value.Record]]

      ac0 <- eitherT(
        handleFutureFailure(commandService.create(jwt, jwtPayload, cmd))
      ): ET[domain.ActiveContract.WithParty[lav1.value.Value]]

      ac1 <- enrichParties(ac0): ET[domain.ActiveContract.WithPartyDetails[lav1.value.Value]]

      jsVal <- either(encoder.encodeV(ac1).leftMap(e => ServerError(e.shows))): ET[JsValue]

    } yield jsVal

  def exercise(req: HttpRequest): ET[JsValue] =
    for {
      t3 <- eitherT(input(req)): ET[(Jwt, JwtPayload, String)]

      (jwt, jwtPayload, reqBody) = t3

      cmd <- either(
        decoder
          .decodeExerciseCommand(reqBody)
          .leftMap(e => InvalidUserInput(e.shows))
      ): ET[domain.ExerciseCommand[LfValue, domain.ContractLocator[LfValue]]]

      resolvedRef <- eitherT(
        resolveReference(jwt, jwtPayload, cmd.reference)
      ): ET[domain.ResolvedContractRef[ApiValue]]

      apiArg <- either(lfValueToApiValue(cmd.argument)): ET[ApiValue]

      resolvedCmd = cmd.copy(argument = apiArg, reference = resolvedRef)

      apiResp <- eitherT(
        handleFutureFailure(commandService.exercise(jwt, jwtPayload, resolvedCmd))
      ): ET[domain.ExerciseResponse.WithParty[ApiValue]]

      lfResp <- either(
        apiResp.traverse(apiValueToLfValue)
      ): ET[domain.ExerciseResponse.WithParty[LfValue]]

      jsResp0 <- either(
        lfResp.traverse(lfValueToJsValue)
      ): ET[domain.ExerciseResponse.WithParty[JsValue]]

      jsResp1 <- enrichParties(jsResp0): ET[domain.ExerciseResponse.WithPartyDetails[JsValue]]

      jsVal <- either(SprayJson.encode(jsResp1).leftMap(e => ServerError(e.shows))): ET[JsValue]

    } yield jsVal

  def fetch(req: HttpRequest): ET[JsValue] =
    for {
      input <- FutureUtil.eitherT(input(req)): ET[(Jwt, JwtPayload, String)]

      (jwt, jwtPayload, reqBody) = input

      _ = logger.debug(s"/v1/fetch reqBody: $reqBody")

      cl <- either(
        decoder
          .decodeContractLocator(reqBody)
          .leftMap(e => InvalidUserInput(e.shows))
      ): ET[domain.ContractLocator[LfValue]]

      _ = logger.debug(s"/v1/fetch cl: $cl")

      ac0 <- eitherT(
        handleFutureFailure(contractsService.lookup(jwt, jwtPayload, cl))
      ): ET[Option[domain.ActiveContract.WithParty[LfValue]]]

      ac1 <- ac0.traverse(enrichParties(_)): ET[
        Option[domain.ActiveContract.WithPartyDetails[LfValue]]]

      jsVal <- either(
        ac1.cata(x => lfAcToJsValue(x).leftMap(e => ServerError(e.shows)), \/-(JsNull))
      ): ET[JsValue]

    } yield jsVal

  def retrieveAll(req: HttpRequest): Future[Error \/ SearchResult[Error \/ JsValue]] =
    inputAndPartyMap(req).map {
      _.map {
        case (jwt, jwtPayload, _, partyMap) =>
          val result
            : SearchResult[ContractsService.Error \/ domain.ActiveContract.WithParty[LfValue]] =
            contractsService
              .retrieveAll(jwt, jwtPayload)

          val jsValSource: Source[Error \/ JsValue, NotUsed] = result.source
            .via(handleSourceFailure)
            .map(_.flatMap(ac => lfAcToJsValue(ac.leftMap(resolveParty(partyMap)))))

          result.copy(source = jsValSource): SearchResult[Error \/ JsValue]
      }
    }

  def query(req: HttpRequest): Future[Error \/ SearchResult[Error \/ JsValue]] =
    inputAndPartyMap(req).map {
      _.flatMap {
        case (jwt, jwtPayload, reqBody, partyMap) =>
          SprayJson
            .decode[domain.GetActiveContractsRequest](reqBody)
            .leftMap(e => InvalidUserInput(e.shows))
            .map { cmd =>
              val result
                : SearchResult[ContractsService.Error \/ domain.ActiveContract.WithParty[JsValue]] =
                contractsService
                  .search(jwt, jwtPayload, cmd)

              val jsValSource: Source[Error \/ JsValue, NotUsed] = result.source
                .via(handleSourceFailure)
                .map(_.flatMap(ac => jsAcToJsValue(ac.leftMap(resolveParty(partyMap)))))

              result.copy(source = jsValSource): SearchResult[Error \/ JsValue]
            }
      }
    }

  def parties(req: HttpRequest): ET[JsValue] =
    for {
      _ <- FutureUtil.eitherT(input(req)): ET[(Jwt, JwtPayload, String)]
      ps <- rightT(partiesService.allParties()): ET[List[domain.PartyDetails]]
      jsVal <- either(SprayJson.encode(ps)).leftMap(e => ServerError(e.shows)): ET[JsValue]
    } yield jsVal

  private def handleFutureFailure[A: Show, B](fa: Future[A \/ B]): Future[ServerError \/ B] =
    fa.map(a => a.leftMap(e => ServerError(e.shows))).recover {
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
      .fromFunction((_: E \/ A).leftMap(e => ServerError(e.shows)))
      .recover {
        case NonFatal(e) =>
          logger.error("Source failed", e)
          -\/(ServerError(e.description))
      }

  private def httpResponse(output: ET[JsValue]): Future[HttpResponse] = {
    val fa: Future[Error \/ JsValue] = output.run
    fa.map {
        case \/-(a) => httpResponseOk(a)
        case -\/(e) => httpResponseError(e)
      }
      .recover {
        case NonFatal(e) => httpResponseError(ServerError(e.description))
      }
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
    val jsValSource: Source[Error \/ JsValue, NotUsed] = searchResult.source

    val warnings: Option[domain.UnknownTemplateIds] =
      if (searchResult.unresolvedTemplateIds.nonEmpty)
        Some(domain.UnknownTemplateIds(searchResult.unresolvedTemplateIds.toList))
      else None

    val jsValWarnings: Option[JsValue] = warnings.map(_.toJson)

    HttpResponse(
      status = StatusCodes.OK,
      entity = HttpEntity
        .CloseDelimited(
          ContentTypes.`application/json`,
          ResponseFormats.resultJsObject(jsValSource, jsValWarnings))
    )
  }

  private[http] def input(req: HttpRequest): Future[Unauthorized \/ (Jwt, JwtPayload, String)] = {
    findJwt(req).flatMap(decodeAndParsePayload(_, decodeJwt)) match {
      case e @ -\/(_) =>
        discard { req.entity.discardBytes(mat) }
        Future.successful(e)
      case \/-((j, p)) =>
        req.entity
          .toStrict(maxTimeToCollectRequest)
          .map(b => \/-((j, p, b.data.utf8String)))
    }
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

  private def enrichParties[F[_, _], B](fab: F[domain.Party, B])(
      implicit ev: Bitraverse[F]): ET[F[domain.PartyDetails, B]] = {
    for {
      partiesMap <- rightT(
        partiesService.allParties().map(xs => PartiesService.buildPartiesMap(xs))
      ): ET[PartiesService.PartyMap]

      fxb = ev.leftMap(fab)(resolveParty(partiesMap)): F[domain.PartyDetails, B]

    } yield fxb
  }

  // TODO(Leo): memoize it
  private def partiesMap(): Future[Error \/ PartiesService.PartyMap] =
    handleFutureFailure(partiesService.allParties().map(xs => PartiesService.buildPartiesMap(xs)))

  private def inputAndPartyMap(
      req: HttpRequest): Future[Error \/ (Jwt, JwtPayload, String, PartiesService.PartyMap)] = {
    val inputF = input(req)
    val mapF = partiesMap()
    ^(inputF, mapF) { (inputE, mapE) =>
      ^(inputE, mapE)((input, map) => (input._1, input._2, input._3, map))
    }
  }
}

object Endpoints {
  import json.JsonProtocol._

  private type ET[A] = EitherT[Future, Error, A]

  private type ApiValue = lav1.value.Value

  private type LfValue = lf.value.Value[lf.value.Value.AbsoluteContractId]

  private def apiValueToLfValue(a: ApiValue): Error \/ LfValue =
    ApiValueToLfValueConverter.apiValueToLfValue(a).leftMap(e => ServerError(e.shows))

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.fromTryCatchNonFatal(LfValueCodec.apiValueToJsValue(a)).leftMap(e =>
      ServerError(e.description))

  private def lfValueToApiValue(a: LfValue): Error \/ ApiValue =
    JsValueToApiValueConverter.lfValueToApiValue(a).leftMap(e => ServerError(e.shows))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def lfAcToJsValue(
      a: domain.ActiveContract.WithPartyDetails[LfValue]): Error \/ JsValue = {
    for {
      b <- a.traverse(lfValueToJsValue): Error \/ domain.ActiveContract.WithPartyDetails[JsValue]
      c <- SprayJson.encode(b).leftMap(e => ServerError(e.shows))
    } yield c
  }

  private def jsAcToJsValue(a: domain.ActiveContract.WithPartyDetails[JsValue]): Error \/ JsValue =
    SprayJson.encode(a).leftMap(e => ServerError(e.shows))

  private def resolveParty(map: Map[domain.Party, domain.PartyDetails])(
      identifier: domain.Party): domain.PartyDetails =
    map.getOrElse(identifier, domain.PartyDetails(identifier, None))
}
