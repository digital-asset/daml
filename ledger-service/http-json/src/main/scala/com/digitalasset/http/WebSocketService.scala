// Copyright (c) 2019 The DAML Authors. All rights reserved.
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
import util.ApiValueToLfValueConverter.apiValueToLfValue
import json.JsonProtocol.LfValueCodec.{apiValueToJsValue => lfValueToJsValue}
import query.ValuePredicate.LfV
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.{v1 => api}

import com.typesafe.scalalogging.LazyLogging
import scalaz.Liskov.<~<
import scalaz.syntax.show._
import scalaz.syntax.std.boolean._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.{-\/, \/, \/-, Show}
import spray.json.{JsObject, JsString, JsValue}

import scala.collection.SeqLike
import scala.collection.immutable.Set
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object WebSocketService {
  private type CompiledQueries = Map[domain.TemplateId.RequiredPkg, LfV => Boolean]
  val heartBeat: String = JsObject("heartbeat" -> JsString("ping")).compactPrint
  val emptyGetActiveContractsRequest = domain.GetActiveContractsRequest(Set.empty, Map.empty)
  private val numConns = new java.util.concurrent.atomic.AtomicInteger(0)

  private implicit final class `\\/ WSS extras`[L, R](private val self: L \/ R) extends AnyVal {
    def liftErr[M](f: String => M)(implicit L: Show[L]): M \/ R =
      self leftMap (e => f(e.shows))
  }

  private implicit final class `Seq WSS extras`[A, Self](private val self: SeqLike[A, Self])
      extends AnyVal {
    import collection.generic.CanBuildFrom
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    def partitionMap[E, B, Es, That](f: A => E \/ B)(
        implicit es: CanBuildFrom[Self, E, Es],
        that: CanBuildFrom[Self, B, That]): (Es, That) = {
      val esb = es(self.repr)
      val thatb = that(self.repr)
      self foreach { a =>
        f(a) fold (esb.+=, thatb.+=)
      }
      (esb.result, thatb.result)
    }

  }

  private final case class StepAndErrors[+LfV](
      errors: Seq[ServerError],
      step: InsertDeleteStep[domain.ActiveContract[LfV]]) {
    import json.JsonProtocol._, spray.json._
    def render(implicit lfv: LfV <~< JsValue): JsValue = {
      def opr[V <: Iterable[_]: JsonWriter](v: V) =
        v.nonEmpty option v.toJson
      JsObject(
        Map(
          "errors" -> opr(errors.map(_.message)),
          "add" -> lfv.subst[Lambda[`-i` => Vector[domain.ActiveContract[i]] => Option[JsValue]]](
            opr(_))(step.inserts),
          "remove" -> opr(step.deletes)
        ) collect { case (k, Some(v)) => (k, v) })
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
    resolveTemplateIds: PackageService.ResolveTemplateIds,
    encoder: DomainJsonEncoder,
    decoder: DomainJsonDecoder,
    wsConfig: Option[WebsocketConfig])(implicit mat: Materializer, ec: ExecutionContext)
    extends LazyLogging {

  import WebSocketService._
  import com.digitalasset.http.json.JsonProtocol._

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
          wsErrorMessage(s"Error parsing your input message to a valid Json request: $e"))
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
      request: GetActiveContractsRequest): Source[Message, NotUsed] =
    resolveTemplateIds(request.templateIds) match {
      case \/-(ids) =>
        contractsService
          .insertDeleteStepSource(jwt, jwtPayload.party, ids)
          .via(convertFilterContracts(prepareFilters(ids, request.query)))
          .filter(_.nonEmpty)
          .map(sae => TextMessage(sae.render.compactPrint))
      case -\/(_) =>
        Source.single(wsErrorMessage("Cannot find templateIds " + request.templateIds.toString))
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
              compiledQueries.get(acLfv.templateId).exists(_(acLfv.argument))
            }))
      }
      .via(conflation)
      .map(sae => sae copy (step = sae.step.mapPreservingIds(_ map lfValueToJsValue)))

}
