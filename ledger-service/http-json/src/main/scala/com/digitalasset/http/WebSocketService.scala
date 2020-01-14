// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.Materializer
import com.digitalasset.http.EndpointsCompanion._
import com.digitalasset.http.domain.{GetActiveContractsRequest, JwtPayload}
import com.digitalasset.http.json.SprayJson.JsonReaderError
import com.digitalasset.http.json.{DomainJsonDecoder, DomainJsonEncoder, SprayJson}
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.{v1 => api}
import com.digitalasset.ledger.client.binding.offset.LedgerOffsetOrdering
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import com.typesafe.scalalogging.LazyLogging
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{-\/, \/, \/-}
import spray.json.{JsObject, JsString, JsValue}

import scala.collection.immutable.Set
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object WebSocketService {
  val heartBeat: String = JsObject("heartbeat" -> JsString("ping")).toString
  val emptyGetActiveContractsRequest = domain.GetActiveContractsRequest(Set.empty, Map.empty)
  private val numConns = new java.util.concurrent.atomic.AtomicInteger(0)
}

class WebSocketService(
    transactionClient: TransactionClient,
    resolveTemplateId: PackageService.ResolveTemplateId,
    encoder: DomainJsonEncoder,
    decoder: DomainJsonDecoder,
    wsConfig: WebsocketConfig)(implicit mat: Materializer, ec: ExecutionContext)
    extends LazyLogging {

  import WebSocketService._
  import com.digitalasset.http.json.JsonProtocol._

  @SuppressWarnings(
    Array("org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.JavaSerializable"))
  private[http] def transactionMessageHandler(
      jwt: Jwt,
      jwtPayload: JwtPayload): Flow[Message, Message, _] = {
    import scala.concurrent.duration._

    wsMessageHandler(jwt, jwtPayload)
      .takeWithin(wsConfig.maxDuration)
      .throttle(wsConfig.throttleElem, wsConfig.throttlePer, wsConfig.maxBurst, wsConfig.mode)
      .keepAlive(5.seconds, () => TextMessage.Strict(heartBeat))
      .watchTermination() { (_, future) =>
        numConns.incrementAndGet
        logger.info(s"New websocket client has connected, current number of clients:$numConns")
        future onComplete {
          case Success(_) =>
            numConns.decrementAndGet
            logger.info(s"Websocket client has disconnected. Current number of clients: $numConns")
          case Failure(ex) =>
            numConns.decrementAndGet
            logger.info(
              s"Websocket client interrupted on Failure: ${ex.getMessage}. remaining number of clients: $numConns")
        }
      }
  }

  private def wsMessageHandler(
      jwt: Jwt,
      jwtPayload: JwtPayload): Flow[Message, Message, NotUsed] = {
    Flow[Message]
      .flatMapConcat {
        case msg: TextMessage.Strict => generateOutgoingMessage(jwt, jwtPayload, msg)
        case _ =>
          Source.single(
            wsErrorMessage("Cannot process your input, Expect a single strict JSON message"))
      }
  }

  private def generateOutgoingMessage(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      incoming: TextMessage.Strict): Source[Message, NotUsed] = {
    val maybeIncomingJs = SprayJson.parse(incoming.text).toOption
    parseActiveContractsRequest(maybeIncomingJs)
      .leftMap(e => InvalidUserInput(e.shows)) match {
      case \/-(req) => getTransactionSourceForParty(jwt, jwtPayload, req)
      case -\/(e) =>
        Source.single(
          wsErrorMessage(s"Error happend parsing your input message to a valid Json request: $e"))
    }
  }

  private def parseActiveContractsRequest(
      incoming: Option[JsValue]
  ): SprayJson.JsonReaderError \/ GetActiveContractsRequest = {
    incoming match {
      case Some(JsObject.empty) => \/-(emptyGetActiveContractsRequest)
      case Some(jsObj) => SprayJson.decode[GetActiveContractsRequest](jsObj)
      case None => -\/(JsonReaderError("None", "please send a valid json request"))
    }
  }

  private def getTransactionSourceForParty(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      request: GetActiveContractsRequest): Source[Message, NotUsed] = {
    import com.digitalasset.http.util.Transactions._

    resolveRequiredTemplateIds(request.templateIds) match {
      case Some(ids) =>
        val filter = transactionFilterFor(jwtPayload.party, ids)
        transactionClient
          .getTransactions(LedgerOffsetOrdering.ledgerBegin, None, transactionFilter = filter) // TODO: make offSet pass along with client message
          .via(Flow[Transaction].filter(_.events.nonEmpty))
          .map(tx => {
            lfVToJson(tx) match {
              case \/-(a) => TextMessage(JsObject("transaction" -> a).toString)
              case -\/(e) => wsErrorMessage(e.shows)
            }
          })
      case None =>
        Source.single(
          wsErrorMessage("Cannot find one of templateIds " + request.templateIds.toString))
    }
  }

  private[http] def wsErrorMessage(errorMsg: String): TextMessage.Strict = {
    TextMessage(
      JsObject("error" -> JsString(errorMsg)).toString
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def lfVToJson(tx: api.transaction.Transaction): Error \/ JsValue = {
    import com.digitalasset.http.util.Commands._
    import scalaz.std.list._
    contracts(tx)
      .leftMap(e => ServerError(e.shows))
      .flatMap(
        _.traverse(v => encoder.encodeV(v))
          .leftMap(e => ServerError(e.shows))
          .flatMap(as => encodeList(as))
      )
  }

  private def resolveRequiredTemplateIds(
      xs: Set[domain.TemplateId.OptionalPkg]): Option[List[domain.TemplateId.RequiredPkg]] = {
    import scalaz.std.list._
    import scalaz.std.option._
    xs.toList.traverse(resolveTemplateId)
  }
}
