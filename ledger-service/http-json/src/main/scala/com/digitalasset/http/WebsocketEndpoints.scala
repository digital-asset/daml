package com.digitalasset.http

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws.{Message, UpgradeToWebSocket}
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import com.digitalasset.http.json.{DomainJsonDecoder, DomainJsonEncoder}
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.std.option._
import scalaz.{-\/, \/, \/-}
import scala.concurrent.{ExecutionContext, Future}
import EndpointsCompanion._

object WebsocketEndpoints {
  private val tokenPrefix: String = "jwt.token."
  private val wsProtocol: String = "daml.ws.auth"
}

class WebsocketEndpoints(ledgerId: lar.LedgerId,
                         decodeJwt: ValidateJwt, //TODO: move to shared trait with enpoints
                         webSocketService: WebSocketService,
                         encoder: DomainJsonEncoder,
                         decoder: DomainJsonDecoder)(
                          implicit mat: Materializer, ec: ExecutionContext) extends StrictLogging {

  import WebsocketEndpoints._

  lazy val transactionWebSocket: PartialFunction[HttpRequest, Future[HttpResponse]] = {
    case req@HttpRequest(GET, Uri.Path("/transaction/connect"), _, _, _) =>
      req.header[UpgradeToWebSocket] match {
        case Some(upgradeReq) =>
          connect(upgradeReq, Some(wsProtocol))
        case None => Future.successful(httpResponseError(InvalidUserInput(s"Cannot upgrade client's connection to websocket")))
      }
  }

  private[http] def connect(req: UpgradeToWebSocket, protocol: Option[String]): Future[HttpResponse] = {
    protocol.map(req.requestedProtocols.contains(_)) match {
      case Some(valid) if valid => handleWebsocketRequest(req, protocol)
      case _ => Future.successful(httpResponseError(Unauthorized(s"Missing required $tokenPrefix.[token] or $wsProtocol subprotocol")))
    }
  }


  private def handleWebsocketRequest(req: UpgradeToWebSocket, protocol: Option[String]):Future[HttpResponse] = {
    findJwtFromSubProtocol(req).flatMap(decodeAndParsePayload(_, decodeJwt)) match {
      case \/-((jwt, jwtPayload)) =>
        val handler: Flow[Message, Message, _] = webSocketService.transactionMessageHandler(jwt, jwtPayload)
        Future.successful(req.handleMessages(handler, protocol))
      case -\/(e) => Future.successful(httpResponseError(e))
    }
  }

  private def findJwtFromSubProtocol(upgradeToWebSocket: UpgradeToWebSocket): Unauthorized \/ Jwt = {
    upgradeToWebSocket.requestedProtocols.collectFirst {
      case p if p startsWith tokenPrefix => Jwt(p.replace(tokenPrefix, ""))
    }.toRightDisjunction(Unauthorized(s"Missing required $tokenPrefix.[token] in subprotocol"))
  }

}
