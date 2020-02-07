// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.Materializer
import com.digitalasset.http.EndpointsCompanion._
import com.digitalasset.http.domain.{JwtPayload, SearchForeverRequest}
import com.digitalasset.http.json.{DomainJsonDecoder, DomainJsonEncoder, JsonProtocol, SprayJson}
import com.digitalasset.http.LedgerClientJwt.Terminates
import util.ApiValueToLfValueConverter.apiValueToLfValue
import util.Collections._
import util.InsertDeleteStep
import json.JsonProtocol.LfValueCodec.{apiValueToJsValue => lfValueToJsValue}
import query.ValuePredicate.{LfV, TypeLookup}
import com.digitalasset.jwt.domain.Jwt
import com.typesafe.scalalogging.LazyLogging
import scalaz.{-\/, Liskov, NonEmptyList, Show, \/, \/-}
import Liskov.<~<
import com.digitalasset.http.query.ValuePredicate
import scalaz.syntax.bifunctor._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.std.map._
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.{-\/, Show, \/, \/-}
import spray.json.{JsObject, JsString, JsValue}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object WebSocketService {
  private type CompiledQueries = Map[domain.TemplateId.RequiredPkg, LfV => Boolean]

  private type StreamPredicate[+Positive] = (
      Set[domain.TemplateId.RequiredPkg],
      Set[domain.TemplateId.OptionalPkg],
      domain.ActiveContract[LfV] => Option[Positive],
  )

  val heartBeat: String = JsObject("heartbeat" -> JsString("ping")).compactPrint

  private implicit final class `\\/ WSS extras`[L, R](private val self: L \/ R) extends AnyVal {
    def liftErr[M](f: String => M)(implicit L: Show[L]): M \/ R =
      self leftMap (e => f(e.shows))
  }

  private final case class StepAndErrors[+Pos, +LfV](
      errors: Seq[ServerError],
      step: InsertDeleteStep[domain.ArchivedContract, (domain.ActiveContract[LfV], Pos)]) {
    import json.JsonProtocol._, spray.json._
    def render(implicit lfv: LfV <~< JsValue, pos: Pos <~< Map[String, JsValue]): JsValue = {
      def inj[V: JsonWriter](ctor: String, v: V) = JsObject(ctor -> v.toJson)
      val thisw =
        Liskov.lift2[StepAndErrors, Pos, Map[String, JsValue], LfV, JsValue](pos, lfv)(this)
      val events = JsArray(
        step.deletes.valuesIterator.map(inj("archived", _)).toVector
          ++ thisw.step.inserts.map {
            case (ac, pos) =>
              val acj = inj("created", ac)
              acj copy (fields = acj.fields ++ pos)
          } ++ errors.map(e => inj("error", e.message)))
      JsObject(("events", events))
    }

    def append[P >: Pos, A >: LfV](o: StepAndErrors[P, A]): StepAndErrors[P, A] =
      StepAndErrors(errors ++ o.errors, step append o.step)

    def mapLfv[A](f: LfV => A): StepAndErrors[Pos, A] =
      copy(step = step mapPreservingIds (_ leftMap (_ map f)))

    def mapPos[P](f: Pos => P): StepAndErrors[P, LfV] =
      copy(step = step mapPreservingIds (_ rightMap f))

    def nonEmpty: Boolean = errors.nonEmpty || step.nonEmpty
  }

  private def conflation[P, A]: Flow[StepAndErrors[P, A], StepAndErrors[P, A], NotUsed] =
    Flow[StepAndErrors[P, A]]
      .batchWeighted(max = 200, costFn = {
        case StepAndErrors(errors, InsertDeleteStep(inserts, deletes)) =>
          errors.length.toLong + (inserts.length * 2) + deletes.size
      }, identity)(_ append _)

  trait StreamQuery[A] {

    /** Extra data on success of a predicate. */
    type Positive

    def parse(decoder: DomainJsonDecoder, str: String): Error \/ A

    def allowPhantonArchives: Boolean

    def predicate(
        request: A,
        resolveTemplateId: PackageService.ResolveTemplateId,
        lookupType: ValuePredicate.TypeLookup): StreamPredicate[Positive]

    def renderCreatedMetadata(p: Positive): Map[String, JsValue]
  }

  implicit val SearchForeverRequestWithStreamQuery: StreamQuery[domain.SearchForeverRequest] =
    new StreamQuery[domain.SearchForeverRequest] {

      type Positive = NonEmptyList[Int]

      override def parse(decoder: DomainJsonDecoder, str: String): Error \/ SearchForeverRequest = {
        import JsonProtocol._
        SprayJson
          .decode[SearchForeverRequest](str)
          .liftErr(InvalidUserInput)
      }

      override def allowPhantonArchives: Boolean = true

      override def predicate(
          request: SearchForeverRequest,
          resolveTemplateId: PackageService.ResolveTemplateId,
          lookupType: ValuePredicate.TypeLookup): StreamPredicate[Positive] = {

        import util.Collections._

        val (resolved, unresolved, q) = request.queries.zipWithIndex
          .foldMap {
            case (gacr, ix) =>
              val (resolved, unresolved) =
                gacr.templateIds.partitionMap(x => resolveTemplateId(x) toLeftDisjunction x)
              val q: CompiledQueries = prepareFilters(resolved, gacr.query, lookupType)
              (resolved, unresolved, q transform ((_, p) => NonEmptyList((p, ix))))
          }
        val fn: domain.ActiveContract[LfV] => Option[Positive] = { a =>
          q.get(a.templateId).flatMap { preds =>
            preds.collect(Function unlift { case (p, ix) => p(a.payload) option ix })
          }
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

      override def renderCreatedMetadata(p: Positive) =
        Map {
          import spray.json._, JsonProtocol._
          "matchedQueries" -> p.toJson
        }
    }

  implicit val EnrichedContractKeyWithStreamQuery
    : StreamQuery[List[domain.EnrichedContractKey[LfV]]] =
    new StreamQuery[List[domain.EnrichedContractKey[LfV]]] {

      type Positive = Unit

      import JsonProtocol._

      @SuppressWarnings(Array("org.wartremover.warts.Any"))
      override def parse(
          decoder: DomainJsonDecoder,
          str: String): Error \/ List[domain.EnrichedContractKey[LfV]] =
        for {
          as <- SprayJson
            .decode[List[domain.EnrichedContractKey[JsValue]]](str)
            .liftErr(InvalidUserInput)
          bs = as.map(a => decodeWithFallback(decoder, a))
        } yield bs

      private def decodeWithFallback(
          decoder: DomainJsonDecoder,
          a: domain.EnrichedContractKey[JsValue]): domain.EnrichedContractKey[LfV] =
        decoder
          .decodeUnderlyingValuesToLf(a)
          .valueOr(_ => a.map(_ => com.digitalasset.daml.lf.value.Value.ValueUnit)) // unit will not match any key

      override def allowPhantonArchives: Boolean = false

      override def predicate(
          request: List[domain.EnrichedContractKey[LfV]],
          resolveTemplateId: PackageService.ResolveTemplateId,
          lookupType: TypeLookup): StreamPredicate[Positive] = {

        import util.Collections._

        val (resolvedWithKey, unresolved) =
          request.toSet.partitionMap { x: domain.EnrichedContractKey[LfV] =>
            resolveTemplateId(x.templateId).map((_, x.key)).toLeftDisjunction(x.templateId)
          }

        val q: Map[domain.TemplateId.RequiredPkg, LfV] = resolvedWithKey.toMap
        val fn: domain.ActiveContract[LfV] => Option[Positive] = { a =>
          if (q.get(a.templateId).exists(k => domain.ActiveContract.matchesKey(k)(a)))
            Some(())
          else None
        }
        (q.keySet, unresolved, fn)
      }

      override def renderCreatedMetadata(p: Unit) = Map.empty
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
        .via(removePhantomArchives(remove = !Q.allowPhantonArchives))
        .map(_.mapPos(Q.renderCreatedMetadata).render)
        .prepend(reportUnresolvedTemplateIds(unresolved))
        .map(jsv => TextMessage(jsv.compactPrint))
    } else {
      Source.single(
        wsErrorMessage(
          s"Cannot resolve any templateId from request: ${request: A}, " +
            s"unresolved templateIds: ${unresolved: Set[domain.TemplateId.OptionalPkg]}"))
    }
  }

  private def removePhantomArchives[A, B](remove: Boolean) =
    if (remove) removePhantomArchives_[A, B]
    else Flow[StepAndErrors[A, B]]

  private def removePhantomArchives_[A, B]
    : Flow[StepAndErrors[A, B], StepAndErrors[A, B], NotUsed] =
    Flow[StepAndErrors[A, B]]
      .scan((Set.empty[String], Option.empty[StepAndErrors[A, B]])) {
        case ((s0, _), a0) =>
          val newInserts: Vector[String] = a0.step.inserts.map(_._1.contractId.unwrap)
          val (deletesToEmit, deletesToHold) = s0 partition a0.step.deletes.keySet
          val s1: Set[String] = deletesToHold ++ newInserts
          val a1 = a0.copy(step = a0.step.copy(deletes = a0.step.deletes filterKeys deletesToEmit))

          (s1, if (a1.nonEmpty) Some(a1) else None)
      }
      .collect { case (_, Some(x)) => x }

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
  private def convertFilterContracts[Pos](fn: domain.ActiveContract[LfV] => Option[Pos])
    : Flow[InsertDeleteStep.LAV1, StepAndErrors[Pos, JsValue], NotUsed] =
    Flow
      .fromFunction { step: InsertDeleteStep.LAV1 =>
        val (errors, cs) = step.inserts
          .partitionMap { ce =>
            domain.ActiveContract
              .fromLedgerApi(ce)
              .liftErr(ServerError)
              .flatMap(_.traverse(apiValueToLfValue).liftErr(ServerError))
          }
        val (aerrors, archs) = step.deletes
          .partitionMap {
            case (k, ae) =>
              domain.ArchivedContract
                .fromLedgerApi(ae)
                .liftErr(ServerError)
                .map((k, _))
          }
        StepAndErrors(
          errors ++ aerrors,
          step copy (inserts = (cs: Vector[domain.ActiveContract[LfV]]).flatMap { ac =>
            fn(ac).map((ac, _)).toList
          }, deletes = archs)
        )
      }
      .via(conflation)
      .map(_ mapLfv lfValueToJsValue)

  private def reportUnresolvedTemplateIds(
      unresolved: Set[domain.TemplateId.OptionalPkg]): Source[JsValue, NotUsed] =
    if (unresolved.isEmpty) Source.empty
    else
      Source.single {
        import spray.json._
        Map("warnings" -> domain.UnknownTemplateIds(unresolved.toList)).toJson
      }
}
