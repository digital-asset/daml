// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.Materializer
import com.digitalasset.http.EndpointsCompanion._
import com.digitalasset.http.domain.{GetActiveContractsRequest, JwtPayload}
import com.digitalasset.http.json.{DomainJsonDecoder, DomainJsonEncoder, SprayJson}
import SprayJson.JsonReaderError
import ContractsFetch.InsertDeleteStep
import com.digitalasset.http.LedgerClientJwt.Terminates
import util.ApiValueToLfValueConverter.apiValueToLfValue
import util.Collections._
import json.JsonProtocol.LfValueCodec.{apiValueToJsValue => lfValueToJsValue}
import query.ValuePredicate.LfV
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.{v1 => api}

import com.typesafe.scalalogging.LazyLogging
import scalaz.Liskov, Liskov.<~<
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.{-\/, \/, \/-, Show}
import spray.json.{JsObject, JsString, JsValue}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object WebSocketService {
  private type CompiledQueries = Map[domain.TemplateId.RequiredPkg, LfV => Boolean]
  private type FetchByKeyRequests = Map[domain.TemplateId.RequiredPkg, LfV]
  val heartBeat: String = JsObject("heartbeat" -> JsString("ping")).compactPrint

  private implicit final class `\\/ WSS extras`[L, R](private val self: L \/ R) extends AnyVal {
    def liftErr[M](f: String => M)(implicit L: Show[L]): M \/ R =
      self leftMap (e => f(e.shows))
  }

  private final case class StepAndErrors[+LfV](
      errors: Seq[ServerError],
      step: InsertDeleteStep[domain.ActiveContract[LfV]]) {
    import json.JsonProtocol._, spray.json._
    def render(implicit lfv: LfV <~< JsValue): JsValue = {
      def inj[V: JsonWriter](ctor: String) = (v: V) => JsObject(ctor -> v.toJson)
      type RF[+i] = Vector[domain.ActiveContract[i]]
      JsArray(
        Liskov.co[RF, LfV, JsValue](lfv)(step.inserts).map(inj("created"))
          ++ step.deletes.map(inj("archived"))
          ++ errors.map(inj[String]("error") compose (_.message)))
    }

    def append[A >: LfV](o: StepAndErrors[A]): StepAndErrors[A] =
      StepAndErrors(errors ++ o.errors, step.appendWithCid(o.step)(_.contractId.unwrap))

    def nonEmpty = errors.nonEmpty || step.nonEmpty
  }

  private def conflation[A]: Flow[StepAndErrors[A], StepAndErrors[A], NotUsed] =
    Flow[StepAndErrors[A]]
      .batchWeighted(max = 200, costFn = {
        case StepAndErrors(errors, InsertDeleteStep(inserts, deletes)) =>
          errors.length.toLong + (inserts.length * 2) + deletes.size
      }, identity)(_ append _)
}

class WebSocketService(
    contractsService: ContractsService,
    resolveTemplateId: PackageService.ResolveTemplateId,
    encoder: DomainJsonEncoder,
    decoder: DomainJsonDecoder,
    wsConfig: Option[WebsocketConfig])(implicit mat: Materializer, ec: ExecutionContext)
    extends LazyLogging {

  import WebSocketService._
  import com.digitalasset.http.json.JsonProtocol._

  private val numConns = new java.util.concurrent.atomic.AtomicInteger(0)

  private[http] def transactionMessageHandler(
      jwt: Jwt,
      jwtPayload: JwtPayload): Flow[Message, Message, _] = {

    wsMessageHandler(jwt, jwtPayload)
      .via(applyConfig(keepAlive = TextMessage.Strict(heartBeat)))
      .via(connCounter)
  }

  private def applyConfig[A](keepAlive: A): Flow[A, A, NotUsed] = {
    val config = wsConfig.getOrElse(Config.DefaultWsConfig)
    Flow[A]
      .takeWithin(config.maxDuration)
      .throttle(config.throttleElem, config.throttlePer, config.maxBurst, config.mode)
      .keepAlive(config.heartBeatPer, () => keepAlive)
  }

  @SuppressWarnings(
    Array("org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.JavaSerializable"))
  private def connCounter[A]: Flow[A, A, NotUsed] =
    Flow[A]
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
        NotUsed
      }

  private val wsReadTimeout = (wsConfig getOrElse Config.DefaultWsConfig).maxDuration

  private def wsMessageHandler(
      jwt: Jwt,
      jwtPayload: JwtPayload): Flow[Message, Message, NotUsed] = {
    Flow[Message]
      .mapAsync(1) {
        case msg: TextMessage => msg.toStrict(wsReadTimeout).map(\/-(_))
        case _ =>
          Future successful -\/(
            Source.single(
              wsErrorMessage("Cannot process your input, Expect a single JSON message"): Message))
      }
      .flatMapConcat { _.map(generateOutgoingMessage(jwt, jwtPayload, _)).merge }
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
          wsErrorMessage(s"Error parsing your input message to a valid Json request: $e"))
    }
  }

  private def parseActiveContractsRequest(
      incoming: Option[JsValue]
  ): SprayJson.JsonReaderError \/ GetActiveContractsRequest = {
    incoming match {
      case Some(jsObj) => SprayJson.decode[GetActiveContractsRequest](jsObj)
      case None => -\/(JsonReaderError("None", "please send a valid json request"))
    }
  }

  private def getTransactionSourceForParty(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      request: GetActiveContractsRequest): Source[Message, NotUsed] =
    resolveRequiredTemplateIds(request.templateIds) match {
      case Some(ids) =>
        contractsService
          .insertDeleteStepSource(jwt, jwtPayload.party, ids, Terminates.Never)
          .via(convertFilterContracts(prepareFilters(ids, request.query)))
          .filter(_.nonEmpty)
          .map(sae => TextMessage(sae.render.compactPrint))
      case None =>
        Source.single(
          wsErrorMessage("Cannot find one of templateIds " + request.templateIds.toString))
    }

  private[http] def wsErrorMessage(errorMsg: String): TextMessage.Strict =
    TextMessage(
      JsObject("error" -> JsString(errorMsg)).compactPrint
    )

  private def prepareFilters(
      ids: Iterable[domain.TemplateId.RequiredPkg],
      queryExpr: Map[String, JsValue]): CompiledQueries =
    ids.iterator.map { tid =>
      (tid, contractsService.valuePredicate(tid, queryExpr).toFunPredicate)
    }.toMap

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def convertFilterContracts(compiledQueries: CompiledQueries)
    : Flow[InsertDeleteStep[api.event.CreatedEvent], StepAndErrors[JsValue], NotUsed] =
    Flow
      .fromFunction { step: InsertDeleteStep[api.event.CreatedEvent] =>
        val (errors, cs) = step.inserts
          .partitionMap { ce =>
            domain.ActiveContract
              .fromLedgerApi(ce)
              .liftErr(ServerError)
              .flatMap(_.traverse(apiValueToLfValue).liftErr(ServerError))
          }
        StepAndErrors(
          errors,
          step copy (inserts = (cs: Vector[domain.ActiveContract[LfV]])
            .filter { acLfv =>
              compiledQueries.get(acLfv.templateId).exists(_(acLfv.payload))
            }))
      }
      .via(conflation)
      .map(sae => sae copy (step = sae.step.mapPreservingIds(_ map lfValueToJsValue)))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def convertFilterContractsByKey(requests: FetchByKeyRequests)
    : Flow[InsertDeleteStep[api.event.CreatedEvent], StepAndErrors[JsValue], NotUsed] =
    Flow
      .fromFunction { step: InsertDeleteStep[api.event.CreatedEvent] =>
        val (errors, cs) = step.inserts
          .partitionMap { ce =>
            domain.ActiveContract
              .fromLedgerApi(ce)
              .liftErr(ServerError)
              .flatMap(_.traverse(apiValueToLfValue).liftErr(ServerError))
          }
        // TODO(Leo): remove deletes, don't need them in this case
        StepAndErrors(
          errors,
          step copy (inserts = (cs: Vector[domain.ActiveContract[LfV]])
            .filter { acLfv =>
              requests
                .get(acLfv.templateId)
                .exists(k => domain.ActiveContract.matchesKey(k)(acLfv))
            })
        )
      }
//      .via(conflation)
      .map(sae => sae copy (step = sae.step.mapPreservingIds(_ map lfValueToJsValue)))

  private def resolveRequiredTemplateIds(
      xs: Set[domain.TemplateId.OptionalPkg]): Option[List[domain.TemplateId.RequiredPkg]] = {
    import scalaz.std.list._
    import scalaz.std.option._
    xs.toList.traverse(resolveTemplateId)
  }
}
