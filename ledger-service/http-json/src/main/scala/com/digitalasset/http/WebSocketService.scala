package com.digitalasset.http

import akka.NotUsed
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.{Materializer, ThrottleMode}
import com.digitalasset.daml.lf
import com.digitalasset.http.EndpointsCompanion._
import com.digitalasset.http.domain.{GetActiveContractsRequest, JwtPayload}
import com.digitalasset.http.json.SprayJson.JsonReaderError
import com.digitalasset.http.json.{DomainJsonDecoder, DomainJsonEncoder, SprayJson}
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.{v1 => api}
import com.digitalasset.ledger.client.binding.offset.LedgerOffsetOrdering
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.{-\/, \/, \/-}
import spray.json.{JsObject, JsString, JsValue}

import scala.collection.immutable.Set

object WebSocketService {
  val heartBeat: String = JsObject("heartbeat" -> JsString("ping")).toString
  val emptyGetActiveContractsRequest = domain.GetActiveContractsRequest(Set.empty, Map.empty)
  private type LfValue = lf.value.Value[lf.value.Value.AbsoluteContractId]

}

class WebSocketService(transactionClient: TransactionClient,
                       resolveTemplateIds: PackageService.ResolveTemplateIds,
                       encoder: DomainJsonEncoder,
                       decoder: DomainJsonDecoder)
                      (implicit mat: Materializer) {
  import WebSocketService._
  import com.digitalasset.http.json.JsonProtocol._

  private[http] def transactionMessageHandler(jwt: Jwt, jwtPayload: JwtPayload) : Flow[Message, Message, _] = {
    wsMessageHandler(jwt, jwtPayload)
  }

  private def wsMessageHandler(jwt: Jwt, jwtPayload: JwtPayload): Flow[Message, Message, NotUsed] = {
    Flow[Message]
      .filterNot(_.equals(TextMessage.Strict(heartBeat)))
      .flatMapConcat {
        case msg: TextMessage.Strict => generateOutgoingMessage(jwt, jwtPayload, msg)
        case _ => Source.single(wsErrorMessage("Cannot process your input, Expect a single strict JSON message"))
      }
  }

  private def generateOutgoingMessage(jwt: Jwt, jwtPayload: JwtPayload, incoming: TextMessage.Strict): Source[Message, NotUsed] = {
    val maybeIncomingJs= SprayJson.parse(incoming.text).toOption
    parseActiveContractsRequest(maybeIncomingJs)
        .leftMap(e => InvalidUserInput(e.shows)) match {
      case \/-(req) => getTransactionSourceForParty(jwt, jwtPayload, req)
      case -\/(e) => Source.single(wsErrorMessage(s"Error happend parsing your input message to a valid Json request: $e"))
    }
  }

  private def parseActiveContractsRequest(
                      incoming: Option[JsValue]
                      ):SprayJson.JsonReaderError \/ GetActiveContractsRequest = {
    incoming match {
      case Some(JsObject.empty) => \/-(emptyGetActiveContractsRequest)
      case Some(jsObj) => SprayJson.decode[GetActiveContractsRequest](jsObj)
      case None => -\/(JsonReaderError("None", "please send a valid json request"))
    }
  }

  private def getTransactionSourceForParty(jwt: Jwt, jwtPayload: JwtPayload, request: GetActiveContractsRequest): Source[Message, NotUsed] = {
    import com.digitalasset.http.util.Transactions._
    import scala.concurrent.duration._

    resolveTemplateIds(request.templateIds) match {
      case \/-(ids) =>
        val filter = transactionFilterFor(jwtPayload.party, ids)
        transactionClient.getTransactions(LedgerOffsetOrdering.ledgerBegin, None, transactionFilter = filter)
          .via(Flow[Transaction].filter(_.events.nonEmpty))
          .map(tx => {
              lfVToJson(tx) match {
                case \/-(a) => TextMessage(a.toString)
                case -\/(e) => wsErrorMessage(e.shows)
              }
          })
          .throttle(10, 1.second, 20, ThrottleMode.Shaping)
          .keepAlive(5.seconds, () => TextMessage.Strict(heartBeat))
      case -\/(_) => Source.single(wsErrorMessage("Cannot find templateIds " + request.templateIds.toString))
    }
  }

  private[http] def wsErrorMessage(errorMsg: String) : TextMessage.Strict = {
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
}
