package com.digitalasset.http

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{Message, UpgradeToWebSocket}
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.digitalasset.http.Endpoints.Error
import com.digitalasset.http.domain.JwtPayload
import com.digitalasset.http.json.{DomainJsonDecoder, DomainJsonEncoder, ResponseFormats, SprayJson}
import com.digitalasset.jwt.domain.{DecodedJwt, Jwt}
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.{-\/, EitherT, \/, \/-}
import spray.json.{JsObject, JsValue}

import scala.concurrent.{ExecutionContext, Future}

object WebsocketEndpoints {
  // TODO: move this to config ?
  private val tokenPrefix: String = "jwt.token."
  private val wsProtocol: String = "daml.ws.auth"

  type ET[A] = EitherT[Future, Error, A]
}

class WebsocketEndpoints(ledgerId: lar.LedgerId,
                         decodeJwt: Endpoints.ValidateJwt, //TODO: move to shared trait with enpoints
                         webSocketService: WebSocketService,
                         encoder: DomainJsonEncoder,
                         decoder: DomainJsonDecoder)(
                          implicit mat: Materializer, ec: ExecutionContext) extends StrictLogging {

  import Endpoints._
  import WebsocketEndpoints._
  import json.JsonProtocol._

  lazy val transactionWebSocket: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case req@HttpRequest(GET, Uri.Path("/transaction/connect"), _, _, _) =>
      req.header[UpgradeToWebSocket] match {
        case Some(upgradeReq) =>
          connect(upgradeReq, Some(wsProtocol))
        case None => websocketResponseError(InvalidUserInput(s"Cannot upgrade client's connection to websocket"))
      }
  }

  private[http] def connect(req: UpgradeToWebSocket, protocol: Option[String]): Future[HttpResponse] = {
    protocol.map(req.requestedProtocols.contains(_)) match {
      case Some(valid) if valid => handleWebsocketRequest(req, protocol)
      case _ => websocketResponseError(Unauthorized(s"Missing required $tokenPrefix.[token] or $wsProtocol subprotocol"))
    }
  }


  private def handleWebsocketRequest(req: UpgradeToWebSocket, protocol: Option[String]):Future[HttpResponse] = {
    findJwtFromSubProtocol(req).flatMap(decodeAndParsePayload) match {
      case \/-((jwt, jwtPayload)) =>
        val handler: Flow[Message, Message, _] = webSocketService.transactionMessageHandler(jwt, jwtPayload)
        Future.successful(req.handleMessages(handler, protocol))
      case -\/(e) => websocketResponseError(e)
    }
  }

  private def findJwtFromSubProtocol(upgradeToWebSocket: UpgradeToWebSocket): Unauthorized \/ Jwt = {
    upgradeToWebSocket.requestedProtocols.collectFirst {
      case p if p startsWith tokenPrefix => Jwt(p.replace(tokenPrefix, ""))
    }.toRightDisjunction(Unauthorized(s"Missing required $tokenPrefix.[token] in subprotocol"))
  }

  // TODO: below methods are duplicated with Endpoints
  private def websocketResponseError(error: Error): Future[HttpResponse] = {
    val (status, msg): (StatusCode, String) = error match {
      case InvalidUserInput(e) => StatusCodes.BadRequest -> e
      case ServerError(e) => StatusCodes.InternalServerError -> e
      case Unauthorized(e) => StatusCodes.Unauthorized -> e
      case NotFound(e) => StatusCodes.NotFound -> e
    }
    val (statusCode, js): (StatusCode, JsObject) = (status, ResponseFormats.errorsJsObject(status, msg))
    Future.successful(HttpResponse(statusCode, entity = HttpEntity.Strict(ContentTypes.`application/json`, format(js))))
  }

  def format(a: JsValue): ByteString = ByteString(a.compactPrint)

  private[http] def decodeAndParsePayload(jwt: Jwt): Unauthorized \/ (Jwt, JwtPayload) =
    for {
      a <- decodeJwt(jwt): Unauthorized \/ DecodedJwt[String]
      p <- parsePayload(a)
    } yield (jwt, p)

  private[http] def parsePayload(jwt: DecodedJwt[String]): Unauthorized \/ JwtPayload =
    SprayJson.decode[JwtPayload](jwt.payload).leftMap(e => Unauthorized(e.shows))


}
