// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.Materializer
import com.digitalasset.http.EndpointsCompanion._
import com.digitalasset.http.domain.{GetActiveContractsRequest, JwtPayload}
import com.digitalasset.http.json.{DomainJsonDecoder, DomainJsonEncoder, JsonProtocol, SprayJson}
import ContractsFetch.InsertDeleteStep
import com.digitalasset.http.LedgerClientJwt.Terminates
import util.ApiValueToLfValueConverter.apiValueToLfValue
import util.Collections._
import json.JsonProtocol.LfValueCodec.{apiValueToJsValue => lfValueToJsValue}
import query.ValuePredicate.{LfV, TypeLookup}
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.{v1 => api}
import com.typesafe.scalalogging.LazyLogging
import scalaz.Liskov
import Liskov.<~<
import com.digitalasset.http.query.ValuePredicate
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.std.list._
import scalaz.{-\/, Show, \/, \/-}
import spray.json.{JsObject, JsString, JsValue}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object WebSocketService {
  private type CompiledQueries = Map[domain.TemplateId.RequiredPkg, LfV => Boolean]

  // TODO(Leo): need a better name, this is more than just a predicate
  private type AcPredicate = (
      Set[domain.TemplateId.RequiredPkg],
      Set[domain.TemplateId.OptionalPkg],
      domain.ActiveContract[LfV] => Boolean,
  )

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

  trait StreamQuery[A] {

    def parse(decoder: DomainJsonDecoder, str: String): Error \/ A

    def predicate(
        request: A,
        resolveTemplateId: PackageService.ResolveTemplateId,
        lookupType: ValuePredicate.TypeLookup): AcPredicate
  }

  implicit val GetActiveContractsRequestWithStreamQuery
    : StreamQuery[domain.GetActiveContractsRequest] =
    new StreamQuery[domain.GetActiveContractsRequest] {

      override def parse(
          decoder: DomainJsonDecoder,
          str: String): Error \/ GetActiveContractsRequest = {
        import JsonProtocol._
        SprayJson
          .decode[GetActiveContractsRequest](str)
          .liftErr(InvalidUserInput)
      }

      override def predicate(
          request: GetActiveContractsRequest,
          resolveTemplateId: PackageService.ResolveTemplateId,
          lookupType: ValuePredicate.TypeLookup): AcPredicate = {

        import util.Collections._

        val (resolved, unresolved) =
          request.templateIds.partitionMap(x => resolveTemplateId(x) toLeftDisjunction x)
        val q: CompiledQueries = prepareFilters(resolved, request.query, lookupType)
        val fn: domain.ActiveContract[LfV] => Boolean = { a =>
          q.get(a.templateId).exists(_(a.payload))
        }

        (resolved, unresolved, fn)
      }

      private def prepareFilters(
          ids: Set[domain.TemplateId.RequiredPkg],
          queryExpr: Map[String, JsValue],
          lookupType: ValuePredicate.TypeLookup
      ): CompiledQueries =
        ids.iterator.map { tid =>
          (tid, ValuePredicate.fromTemplateJsObject(queryExpr, tid, lookupType).toFunPredicate)
        }.toMap
    }

  implicit val EnrichedContractKeyWithStreamQuery
    : StreamQuery[List[domain.EnrichedContractKey[LfV]]] =
    new StreamQuery[List[domain.EnrichedContractKey[LfV]]] {

      import JsonProtocol._

      @SuppressWarnings(Array("org.wartremover.warts.Any"))
      override def parse(
          decoder: DomainJsonDecoder,
          str: String): Error \/ List[domain.EnrichedContractKey[LfV]] = {
        SprayJson
          .decode[List[domain.EnrichedContractKey[JsValue]]](str)
          .liftErr(InvalidUserInput)
          .flatMap { as: List[domain.EnrichedContractKey[JsValue]] =>
            as.traverse(a => decode(decoder)(a))
          }
      }

      private def decode(decoder: DomainJsonDecoder)(
          a: domain.EnrichedContractKey[JsValue]): Error \/ domain.EnrichedContractKey[LfV] =
        decoder.decodeUnderlyingValuesToLf(a).liftErr(InvalidUserInput)

      override def predicate(
          request: List[domain.EnrichedContractKey[LfV]],
          resolveTemplateId: PackageService.ResolveTemplateId,
          lookupType: TypeLookup): AcPredicate = {

        import util.Collections._

        val (resolvedWithKey, unresolved) =
          request.toSet.partitionMap { x: domain.EnrichedContractKey[LfV] =>
            resolveTemplateId(x.templateId).map((_, x.key)).toLeftDisjunction(x.templateId)
          }

        val q: Map[domain.TemplateId.RequiredPkg, LfV] = resolvedWithKey.toMap
        val fn: domain.ActiveContract[LfV] => Boolean = { a =>
          q.get(a.templateId).exists(k => domain.ActiveContract.matchesKey(k)(a))
        }
        (q.keySet, unresolved, fn)
      }
    }
}

class WebSocketService(
    contractsService: ContractsService,
    resolveTemplateId: PackageService.ResolveTemplateId,
    encoder: DomainJsonEncoder,
    decoder: DomainJsonDecoder,
    lookupType: ValuePredicate.TypeLookup,
    wsConfig: Option[WebsocketConfig])(implicit mat: Materializer, ec: ExecutionContext)
    extends LazyLogging {

  import WebSocketService._
  import com.digitalasset.http.json.JsonProtocol._

  private val numConns = new java.util.concurrent.atomic.AtomicInteger(0)

  private[http] def transactionMessageHandler[A: StreamQuery](
      jwt: Jwt,
      jwtPayload: JwtPayload,
  ): Flow[Message, Message, _] =
    wsMessageHandler[A](jwt, jwtPayload)
      .via(applyConfig(keepAlive = TextMessage.Strict(heartBeat)))
      .via(connCounter)

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

  private def wsMessageHandler[A: StreamQuery](
      jwt: Jwt,
      jwtPayload: JwtPayload,
  ): Flow[Message, Message, NotUsed] = {
    val Q = implicitly[StreamQuery[A]]
    Flow[Message]
      .mapAsync(1) {
        case msg: TextMessage =>
          msg.toStrict(wsReadTimeout).map(m => Q.parse(decoder, m.text))
        case _ =>
          Future successful -\/(
            InvalidUserInput("Cannot process your input, Expect a single JSON message"))
      }
      .flatMapConcat {
        case \/-(a) => getTransactionSourceForParty[A](jwt, jwtPayload, a)
        case -\/(e) => Source.single(wsErrorMessage(e.shows))
      }
  }

  private def getTransactionSourceForParty[A: StreamQuery](
      jwt: Jwt,
      jwtPayload: JwtPayload,
      request: A): Source[Message, NotUsed] = {
    val Q = implicitly[StreamQuery[A]]

    val (resolved, unresolved, fn) = Q.predicate(request, resolveTemplateId, lookupType)

    if (resolved.nonEmpty) {
      contractsService
        .insertDeleteStepSource(jwt, jwtPayload.party, resolved.toList, Terminates.Never)
        .via(convertFilterContracts(fn))
        .filter(_.nonEmpty)
        .map(_.render)
        .prepend(reportUnresolvedTemplateIds(unresolved))
        .map(jsv => TextMessage(jsv.compactPrint))
    } else {
      Source.single(
        wsErrorMessage(
          s"Cannot resolve any templateId from request: ${request: A}, " +
            s"unresolved templateIds: ${unresolved: Set[domain.TemplateId.OptionalPkg]}"))
    }
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
  private def convertFilterContracts(fn: domain.ActiveContract[LfV] => Boolean)
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
          step copy (inserts = (cs: Vector[domain.ActiveContract[LfV]]).filter(fn)))
      }
      .via(conflation)
      .map(sae => sae copy (step = sae.step.mapPreservingIds(_ map lfValueToJsValue)))

  private def reportUnresolvedTemplateIds(
      unresolved: Set[domain.TemplateId.OptionalPkg]): Source[JsValue, NotUsed] =
    if (unresolved.isEmpty) Source.empty
    else
      Source.single {
        import spray.json._
        Map("warnings" -> domain.UnknownTemplateIds(unresolved.toList)).toJson
      }
}
