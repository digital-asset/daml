// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.digitalasset.http.json.ResponseFormats._
import com.digitalasset.http.json.SprayJson.decode
import com.digitalasset.http.json.{DomainJsonDecoder, DomainJsonEncoder, SprayJson}
import com.digitalasset.http.util.FutureUtil
import com.digitalasset.jwt.JwtVerifier.VerifyJwt
import com.digitalasset.jwt.domain.{DecodedJwt, Jwt}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import com.typesafe.scalalogging.StrictLogging
import scalaz.std.scalaFuture._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, Show, \/, \/-}
import spray.json._
import scalaz.std.list._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.control.NonFatal

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class Endpoints(
    ledgerId: lar.LedgerId,
    verifyJwt: VerifyJwt,
    commandService: CommandService,
    contractsService: ContractsService,
    encoder: DomainJsonEncoder,
    decoder: DomainJsonDecoder,
    maxTimeToCollectRequest: FiniteDuration = FiniteDuration(5, "seconds"))(
    implicit ec: ExecutionContext,
    mat: Materializer)
    extends StrictLogging {

  import Endpoints._
  import json.JsonProtocol._

  // TODO(Leo) read it from the header
  private val jwtPayload =
    domain.JwtPayload(ledgerId, lar.ApplicationId("applicationId"), lar.Party("Alice"))

  lazy val all: PartialFunction[HttpRequest, HttpResponse] =
    command orElse contracts orElse notFound

  lazy val command: PartialFunction[HttpRequest, HttpResponse] = {
    case req @ HttpRequest(POST, Uri.Path("/command/create"), _, _, _) =>
      val et: ET[JsValue] = for {
        input <- FutureUtil.eitherT(input2(req)): ET[(domain.JwtPayload, String)]

        (jwtPayload, reqBody) = input

        cmd <- FutureUtil
          .either(
            decoder
              .decodeR[domain.CreateCommand](reqBody)
              .leftMap(e => InvalidUserInput(e.shows))): ET[domain.CreateCommand[lav1.value.Record]]

        ac <- FutureUtil.eitherT(handleFutureFailure(commandService.create(jwtPayload, cmd))): ET[
          domain.ActiveContract[lav1.value.Value]]

        jsVal <- FutureUtil
          .either(encoder.encodeV(ac).leftMap(e => ServerError(e.shows))): ET[JsValue]

      } yield jsVal

      httpResponse(et)

    case req @ HttpRequest(POST, Uri.Path("/command/exercise"), _, _, _) =>
      httpResponse(
        input(req)
          .map(decoder.decodeR[domain.ExerciseCommand])
          .mapAsync(1) {
            case -\/(e) => invalidUserInput(e)
            case \/-(c) => handleFutureFailure(commandService.exercise(jwtPayload, c))
          }
          .map { fa =>
            fa.flatMap { as =>
              as.traverse(a => encoder.encodeV(a))
                .leftMap(e => ServerError(e.shows))
                .flatMap(as => encodeList(as))
            }
          }
          .map(formatResult)
      )
  }

  private def invalidUserInput[A: Show, B](a: A): Future[InvalidUserInput \/ B] =
    Future.successful(-\/(InvalidUserInput(a.shows)))

  private def handleFutureFailure[A: Show, B](fa: Future[A \/ B]): Future[ServerError \/ B] =
    fa.map(a => a.leftMap(e => ServerError(e.shows))).recover {
      case NonFatal(e) =>
        logger.error("Future failed", e)
        -\/(ServerError(e.getMessage))
    }

  private def handleFutureFailure[A](fa: Future[A]): Future[ServerError \/ A] =
    fa.map(a => \/-(a)).recover {
      case NonFatal(e) =>
        logger.error("Future failed", e)
        -\/(ServerError(e.getMessage))
    }

  private def encodeList(as: Seq[JsValue]): ServerError \/ JsValue =
    SprayJson.encode(as).leftMap(e => ServerError(e.shows))

  private def formatResult(fa: Error \/ JsValue): ByteString = fa match {
    case \/-(a) => format(resultJsObject(a: JsValue))
    case -\/(InvalidUserInput(e)) => format(errorsJsObject(StatusCodes.BadRequest)(e))
    case -\/(ServerError(e)) => format(errorsJsObject(StatusCodes.InternalServerError)(e))
    case -\/(Unauthorized(e)) => format(errorsJsObject(StatusCodes.Unauthorized)(e))
  }

  private val emptyGetActiveContractsRequest = domain.GetActiveContractsRequest(Set.empty)

  lazy val contracts: PartialFunction[HttpRequest, HttpResponse] = {
    case req @ HttpRequest(GET, Uri.Path("/contracts/lookup"), _, _, _) =>
      httpResponse(
        input(req)
          .map(decoder.decodeV[domain.ContractLookupRequest])
          .mapAsync(1) {
            case -\/(e) => invalidUserInput(e)
            case \/-(c) => handleFutureFailure(contractsService.lookup(jwtPayload, c))
          }
          .map { fa =>
            fa.flatMap {
              case None => \/-(JsObject())
              case Some(x) => encoder.encodeV(x).leftMap(e => ServerError(e.shows))
            }
          }
          .map(formatResult)
      )

    case HttpRequest(GET, Uri.Path("/contracts/search"), _, _, _) =>
      httpResponse(
        handleFutureFailure(contractsService.search(jwtPayload, emptyGetActiveContractsRequest))
          .map { fas: Error \/ Seq[domain.GetActiveContractsResponse[lav1.value.Value]] =>
            fas.flatMap { as =>
              as.toList
                .traverse(a => encoder.encodeV(a))
                .leftMap(e => ServerError(e.shows))
                .flatMap(js => encodeList(js))
            }
          }
          .map(formatResult)
      )

    case req @ HttpRequest(POST, Uri.Path("/contracts/search"), _, _, _) =>
      httpResponse(
        input(req)
          .map(decode[domain.GetActiveContractsRequest])
          .mapAsync(1) {
            case -\/(e) => invalidUserInput(e)
            case \/-(c) => handleFutureFailure(contractsService.search(jwtPayload, c))
          }
          .map { fas: Error \/ Seq[domain.GetActiveContractsResponse[lav1.value.Value]] =>
            fas.flatMap { as =>
              as.toList
                .traverse(a => encoder.encodeV(a))
                .leftMap(e => ServerError(e.shows))
                .flatMap(js => encodeList(js))
            }
          }
          .map(formatResult)
      )
  }

  private def httpResponse(output: ET[JsValue]): HttpResponse = {
    val f: Future[Error \/ JsValue] = output.run
    httpResponse(f.map(formatResult))
  }

  private def httpResponse(output: Future[ByteString]): HttpResponse =
    HttpResponse(entity =
      HttpEntity.CloseDelimited(ContentTypes.`application/json`, Source.fromFuture(output)))

  private def httpResponse(output: Source[ByteString, _]): HttpResponse =
    HttpResponse(entity = HttpEntity.CloseDelimited(ContentTypes.`application/json`, output))

  lazy val notFound: PartialFunction[HttpRequest, HttpResponse] = {
    case HttpRequest(_, _, _, _, _) => HttpResponse(status = StatusCodes.NotFound)
  }

  private def format(a: JsValue): ByteString =
    ByteString(a.compactPrint)

  private[http] def input(req: HttpRequest): Source[String, _] =
    req.entity.dataBytes.fold(ByteString.empty)(_ ++ _).map(_.utf8String).take(1L)

  private[http] def input2(
      req: HttpRequest): Future[Unauthorized \/ (domain.JwtPayload, String)] = {
    findJwt(req).flatMap(verify) match {
      case e @ -\/(_) =>
        req.entity.discardBytes(mat)
        Future.successful(e)
      case \/-(p) =>
        req.entity
          .toStrict(maxTimeToCollectRequest)
          .map(b => \/-(p -> b.data.utf8String))
    }
  }

  private[http] def findJwt(req: HttpRequest): Unauthorized \/ Jwt =
    req.headers
      .collectFirst {
        case Authorization(OAuth2BearerToken(token)) => Jwt(token)
      }
      .toRightDisjunction(Unauthorized("missing Authorization header with OAuth 2.0 Bearer Token"))

  private def verify(jwt: Jwt): Unauthorized \/ domain.JwtPayload =
    for {
      a <- verifyJwt(jwt).leftMap(e => Unauthorized(e.shows)): Unauthorized \/ DecodedJwt[String]
      b <- parsePayload(a)
    } yield b

  private def parsePayload(jwt: DecodedJwt[String]): Unauthorized \/ domain.JwtPayload =
    SprayJson.decode[domain.JwtPayload](jwt.payload).leftMap(e => Unauthorized(e.shows))
}

object Endpoints {

  private type ET[A] = EitherT[Future, Error, A]

  sealed abstract class Error(message: String) extends Product with Serializable

  final case class InvalidUserInput(message: String) extends Error(message)

  final case class Unauthorized(message: String) extends Error(message)

  final case class ServerError(message: String) extends Error(message)

  object Error {
    implicit val ShowInstance: Show[Error] = Show shows {
      case InvalidUserInput(e) => s"Endpoints.InvalidUserInput: ${e: String}"
      case ServerError(e) => s"Endpoints.ServerError: ${e: String}"
      case Unauthorized(e) => s"Endpoints.Unauthorized: ${e: String}"
    }
  }
}
