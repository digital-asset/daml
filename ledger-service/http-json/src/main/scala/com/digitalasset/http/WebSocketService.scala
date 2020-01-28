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
import scalaz.syntax.std.option._
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
      step: InsertDeleteStep[domain.ActiveContract[LfV]],
  ) {
    import json.JsonProtocol._, spray.json._
    def render(implicit lfv: LfV <~< JsValue): JsValue = {
      def inj[V: JsonWriter](ctor: String) = (v: V) => JsObject(ctor -> v.toJson)
      type RF[+i] = Vector[domain.ActiveContract[i]]
      JsArray(
        Liskov.co[RF, LfV, JsValue](lfv)(step.inserts).map(inj("created"))
          ++ step.deletes.map(inj("archived"))
          ++ errors.map(inj[String]("error") compose (_.message)),
      )
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
    wsConfig: Option[WebsocketConfig],
)(implicit mat: Materializer, ec: ExecutionContext)
    extends LazyLogging {

  import WebSocketService._
  import com.digitalasset.http.json.JsonProtocol._

  private val numConns = new java.util.concurrent.atomic.AtomicInteger(0)

  private[http] def transactionMessageHandler(
      jwt: Jwt,
      jwtPayload: JwtPayload,
  ): Flow[Message, Message, _] = {

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
    Array("org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.JavaSerializable"),
  )
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
              s"Websocket client interrupted on Failure: ${ex.getMessage}. remaining number of clients: $numConns",
            )
        }
        NotUsed
      }

  private val wsReadTimeout = (wsConfig getOrElse Config.DefaultWsConfig).maxDuration

  private def wsMessageHandler(
      jwt: Jwt,
      jwtPayload: JwtPayload,
  ): Flow[Message, Message, NotUsed] = {
    Flow[Message]
      .mapAsync(1) {
        case msg: TextMessage => msg.toStrict(wsReadTimeout).map(\/-(_))
        case _ =>
          Future successful -\/(
            Source.single(
              wsErrorMessage("Cannot process your input, Expect a single JSON message"): Message,
            ),
          )
      }
      .flatMapConcat { _.map(generateOutgoingMessage(jwt, jwtPayload, _)).merge }
  }

  private def generateOutgoingMessage(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      incoming: TextMessage.Strict,
  ): Source[Message, NotUsed] = {
    (for {
      req <- SprayJson.decode[GetActiveContractsRequest](incoming.text).liftErr(InvalidUserInput)
      resolvedTemplateIds <- resolveRequiredTemplateIds(req.templateIds).toRightDisjunction(
        ServerError(s"Cannot resolve one of templateIds: ${req.templateIds.toString}"),
      )
      fn = queryPredicate(prepareFilters(resolvedTemplateIds, req.query)) _
    } yield (resolvedTemplateIds, fn))
      .fold(
        e => Source.single(wsErrorMessage(s"Error handling request: ${incoming.text: String}: $e")),
        idsAndPredicate =>
          getTransactionSourceForParty(
            jwt,
            jwtPayload.party,
            idsAndPredicate._1,
            idsAndPredicate._2,
          ),
      )
  }

  private def getTransactionSourceForParty(
      jwt: Jwt,
      party: domain.Party,
      templateIds: List[domain.TemplateId.RequiredPkg],
      filterPredicate: domain.ActiveContract[LfV] => Boolean,
  ): Source[Message, NotUsed] =
    contractsService
      .insertDeleteStepSource(jwt, party, templateIds, Terminates.Never)
      .via(convertFilterContracts(filterPredicate))
      .filter(_.nonEmpty)
      .map(sae => TextMessage(sae.render.compactPrint))

  private[http] def wsErrorMessage(errorMsg: String): TextMessage.Strict =
    TextMessage(
      JsObject("error" -> JsString(errorMsg)).compactPrint,
    )

  private def prepareFilters(
      ids: Iterable[domain.TemplateId.RequiredPkg],
      queryExpr: Map[String, JsValue],
  ): CompiledQueries =
    ids.iterator.map { tid =>
      (tid, contractsService.valuePredicate(tid, queryExpr).toFunPredicate)
    }.toMap

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def convertFilterContracts(
      fn: domain.ActiveContract[LfV] => Boolean,
  ): Flow[InsertDeleteStep[api.event.CreatedEvent], StepAndErrors[JsValue], NotUsed] =
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
          step copy (inserts = (cs: Vector[domain.ActiveContract[LfV]]).filter(fn)),
        )
      }
      .via(conflation)
      .map(sae => sae copy (step = sae.step.mapPreservingIds(_ map lfValueToJsValue)))

  private def queryPredicate(q: CompiledQueries)(a: domain.ActiveContract[LfV]): Boolean =
    q.get(a.templateId).exists(_(a.payload))

//  private def keyPredicate(r: FetchByKeyRequests)(a: domain.ActiveContract[LfV]): Boolean =
//    r.get(a.templateId)
//      .exists(k => domain.ActiveContract.matchesKey(k)(a))

  private def resolveRequiredTemplateIds(
      xs: Set[domain.TemplateId.OptionalPkg],
  ): Option[List[domain.TemplateId.RequiredPkg]] = {
    import scalaz.std.list._
    import scalaz.std.option._
    xs.toList.traverse(resolveTemplateId)
  }
}
