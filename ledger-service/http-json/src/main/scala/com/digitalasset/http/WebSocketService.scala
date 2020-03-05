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
import util.{ContractStreamStep, InsertDeleteStep}
import json.JsonProtocol.LfValueCodec.{apiValueToJsValue => lfValueToJsValue}
import query.ValuePredicate.{LfV, TypeLookup}
import com.digitalasset.jwt.domain.Jwt
import com.typesafe.scalalogging.LazyLogging
import scalaz.{Liskov, NonEmptyList}
import Liskov.<~<
import com.digitalasset.http.query.ValuePredicate
import scalaz.syntax.bifunctor._
import scalaz.syntax.show._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.std.map._
import scalaz.std.option.{none, some}
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.{-\/, \/, \/-}
import spray.json.{JsObject, JsString, JsValue}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object WebSocketService {
  import util.ErrorOps._

  private type CompiledQueries = Map[domain.TemplateId.RequiredPkg, LfV => Boolean]

  private type StreamPredicate[+Positive] = (
      Set[domain.TemplateId.RequiredPkg],
      Set[domain.TemplateId.OptionalPkg],
      domain.ActiveContract[LfV] => Option[Positive],
  )

  val heartBeat: String = JsObject("heartbeat" -> JsString("ping")).compactPrint

  private def withOptPrefix[I, L, O](fst: I => (L \/ O))(snd: (L, I) => O): Flow[I, O, NotUsed] =
    Flow[I]
      .scan((none[L], none[O])) { (s, i) =>
        val (ol, _) = s
        ol.cata(
          l => (none, some(snd(l, i))),
          fst(i) fold (l => (some(l), none), o => (none, some(o))))
      }
      .collect { case (_, Some(o)) => o }

  private final case class StepAndErrors[+Pos, +LfV](
      errors: Seq[ServerError],
      step: ContractStreamStep[domain.ArchivedContract, (domain.ActiveContract[LfV], Pos)]) {
    import json.JsonProtocol._, spray.json._
    def render(implicit lfv: LfV <~< JsValue, pos: Pos <~< Map[String, JsValue]): JsValue = {
      import ContractStreamStep._
      def inj[V: JsonWriter](ctor: String, v: V) = JsObject(ctor -> v.toJson)
      val InsertDeleteStep(inserts, deletes) =
        Liskov
          .lift2[StepAndErrors, Pos, Map[String, JsValue], LfV, JsValue](pos, lfv)(this)
          .step
          .toInsertDelete

      val events = (deletes.valuesIterator.map(inj("archived", _)).toVector
        ++ inserts.map {
          case (ac, pos) =>
            val acj = inj("created", ac)
            acj copy (fields = acj.fields ++ pos)
        } ++ errors.map(e => inj("error", e.message)))
      val offsetAfter = step match {
        case Acs(_) => None
        case LiveBegin(off) =>
          Some(off.toOption.cata(o => JsString(domain.Offset.unwrap(o)), JsNull: JsValue))
        case Txn(_, off) => Some(JsString(domain.Offset.unwrap(off)))
      }
      JsObject(Map("events" -> JsArray(events)) ++ offsetAfter.map("offset" -> _).toList)
    }

    def append[P >: Pos, A >: LfV](o: StepAndErrors[P, A]): StepAndErrors[P, A] =
      StepAndErrors(errors ++ o.errors, step append o.step)

    def mapLfv[A](f: LfV => A): StepAndErrors[Pos, A] =
      copy(step = step mapPreservingIds (_ leftMap (_ map f)))

    def mapPos[P](f: Pos => P): StepAndErrors[P, LfV] =
      copy(step = step mapPreservingIds (_ rightMap f))

    def nonEmpty: Boolean = errors.nonEmpty || step.nonEmpty
  }

  private def conflation[P, A]: Flow[StepAndErrors[P, A], StepAndErrors[P, A], NotUsed] = {
    val maxCost = 200L
    Flow[StepAndErrors[P, A]]
      .batchWeighted(
        max = maxCost,
        costFn = {
          case StepAndErrors(errors, ContractStreamStep.LiveBegin(_)) =>
            1L + errors.length
          case StepAndErrors(errors, step) =>
            val InsertDeleteStep(inserts, deletes) = step.toInsertDelete
            errors.length.toLong + (inserts.length * 2) + deletes.size
        },
        identity
      )(_ append _)
  }

  trait StreamQuery[A] {

    /** Extra data on success of a predicate. */
    type Positive

    def parse(decoder: DomainJsonDecoder, jv: JsValue): Error \/ A

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

      override def parse(decoder: DomainJsonDecoder, jv: JsValue): Error \/ SearchForeverRequest = {
        import JsonProtocol._
        SprayJson
          .decode[SearchForeverRequest](jv)
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
          jv: JsValue): Error \/ List[domain.EnrichedContractKey[LfV]] =
        for {
          as <- SprayJson
            .decode[List[domain.EnrichedContractKey[JsValue]]](jv)
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
  import util.ErrorOps._
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
          msg.toStrict(wsReadTimeout).map { m =>
            SprayJson.parse(m.text).liftErr(InvalidUserInput)
          }
        case _ =>
          Future successful -\/(
            InvalidUserInput("Cannot process your input, Expect a single JSON message"))
      }
      .via(withOptPrefix { ejv: InvalidUserInput \/ JsValue =>
        \/-(ejv flatMap (Q.parse(decoder, _))): Unit \/ (Error \/ A)
      }((offPrefix, ejv) => ejv flatMap (Q.parse(decoder, _))))
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
    : Flow[StepAndErrors[A, B], StepAndErrors[A, B], NotUsed] = {
    import ContractStreamStep.{LiveBegin, Txn, Acs}
    Flow[StepAndErrors[A, B]]
      .scan((Set.empty[String], Option.empty[StepAndErrors[A, B]])) {
        case ((s0, _), a0 @ StepAndErrors(_, Txn(idstep, _))) =>
          val newInserts: Vector[String] =
            domain.ContractId.unsubst(idstep.inserts.map(_._1.contractId))
          val (deletesToEmit, deletesToHold) = s0 partition idstep.deletes.keySet
          val s1: Set[String] = deletesToHold ++ newInserts
          val a1 = a0.copy(step = a0.step.mapDeletes(_ filterKeys deletesToEmit))

          (s1, if (a1.nonEmpty) Some(a1) else None)

        case ((deletesToHold, _), a0 @ StepAndErrors(_, Acs(inserts))) =>
          val newInserts: Vector[String] = domain.ContractId.unsubst(inserts.map(_._1.contractId))
          val s1: Set[String] = deletesToHold ++ newInserts
          (s1, Some(a0))

        case ((s0, _), a0 @ StepAndErrors(_, LiveBegin(_))) =>
          (s0, Some(a0))
      }
      .collect { case (_, Some(x)) => x }
  }

  private[http] def wsErrorMessage(errorMsg: String): TextMessage.Strict =
    TextMessage(
      JsObject("error" -> JsString(errorMsg)).compactPrint
    )

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def convertFilterContracts[Pos](fn: domain.ActiveContract[LfV] => Option[Pos])
    : Flow[ContractStreamStep.LAV1, StepAndErrors[Pos, JsValue], NotUsed] =
    Flow
      .fromFunction { step: ContractStreamStep.LAV1 =>
        val (aerrors, errors, dstep) = step.partitionBimap(
          ae =>
            domain.ArchivedContract
              .fromLedgerApi(ae)
              .liftErr(ServerError),
          ce =>
            domain.ActiveContract
              .fromLedgerApi(ce)
              .liftErr(ServerError)
              .flatMap(_.traverse(apiValueToLfValue).liftErr(ServerError)),
        )
        StepAndErrors(errors ++ aerrors, dstep mapInserts {
          inserts: Vector[domain.ActiveContract[LfV]] =>
            inserts.flatMap { ac =>
              fn(ac).map((ac, _)).toList
            }
        })
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
