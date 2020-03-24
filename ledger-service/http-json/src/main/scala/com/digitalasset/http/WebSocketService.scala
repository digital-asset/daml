// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.http.scaladsl.model.ws.{Message, TextMessage, BinaryMessage}
import akka.stream.scaladsl.{Flow, Source, Sink}
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
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.std.map._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.{-\/, \/, \/-}
import spray.json.{JsArray, JsObject, JsValue}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object WebSocketService {
  import util.ErrorOps._

  private type CompiledQueries = Map[domain.TemplateId.RequiredPkg, LfV => Boolean]

  private type EventsAndOffset = (Vector[JsObject], Option[JsValue])

  private type StreamPredicate[+Positive] = (
      Set[domain.TemplateId.RequiredPkg],
      Set[domain.TemplateId.OptionalPkg],
      domain.ActiveContract[LfV] => Option[Positive],
  )

  private def withOptPrefix[I, L](prefix: I => Option[L]): Flow[I, (Option[L], I), NotUsed] =
    Flow[I]
      .scan(none[L \/ (Option[L], I)]) { (s, i) =>
        s match {
          case Some(-\/(l)) => Some(\/-((some(l), i)))
          case None | Some(\/-(_)) => Some(prefix(i) toLeftDisjunction ((none, i)))
        }
      }
      .collect { case Some(\/-(oli)) => oli }

  private final case class StepAndErrors[+Pos, +LfV](
      errors: Seq[ServerError],
      step: ContractStreamStep[domain.ArchivedContract, (domain.ActiveContract[LfV], Pos)]) {
    import json.JsonProtocol._, spray.json._
    def render(
        implicit lfv: LfV <~< JsValue,
        pos: Pos <~< Map[String, JsValue]): EventsAndOffset = {
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
      (events, offsetAfter)
    }

    def append[P >: Pos, A >: LfV](o: StepAndErrors[P, A]): StepAndErrors[P, A] =
      StepAndErrors(errors ++ o.errors, step append o.step)

    def mapLfv[A](f: LfV => A): StepAndErrors[Pos, A] =
      copy(step = step mapPreservingIds (_ leftMap (_ map f)))

    def mapPos[P](f: Pos => P): StepAndErrors[P, LfV] =
      copy(step = step mapPreservingIds (_ rightMap f))

    def nonEmpty: Boolean = errors.nonEmpty || step.nonEmpty
  }

  private def renderEvents(events: Vector[JsObject], offset: Option[JsValue]): JsObject =
    JsObject(Map("events" -> JsArray(events)) ++ offset.map("offset" -> _).toList)

  private def readStartingOffset(jv: JsValue): Option[Error \/ domain.StartingOffset] =
    jv match {
      case JsObject(fields) =>
        fields get "offset" map { offJv =>
          import JsonProtocol._
          if (fields.size > 1)
            -\/(InvalidUserInput("offset must be specified as a leading, separate object message"))
          else
            SprayJson
              .decode[String](offJv)
              .liftErr(InvalidUserInput)
              .map(offStr => domain.StartingOffset(domain.Offset(offStr)))
        }
      case _ => None
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

  sealed abstract class StreamQueryReader[A] {
    case class Query[Q](q: Q, alg: StreamQuery[Q])
    def parse(decoder: DomainJsonDecoder, jv: JsValue): Error \/ Query[_]
  }

  sealed trait StreamQuery[A] {

    /** Extra data on success of a predicate. */
    type Positive

    def allowPhantonArchives: Boolean

    def predicate(
        request: A,
        resolveTemplateId: PackageService.ResolveTemplateId,
        lookupType: ValuePredicate.TypeLookup): StreamPredicate[Positive]

    def renderCreatedMetadata(p: Positive): Map[String, JsValue]
  }

  implicit val SearchForeverRequestWithStreamQuery: StreamQueryReader[domain.SearchForeverRequest] =
    new StreamQueryReader[domain.SearchForeverRequest]
    with StreamQuery[domain.SearchForeverRequest] {

      type Positive = NonEmptyList[Int]

      override def parse(decoder: DomainJsonDecoder, jv: JsValue) = {
        import JsonProtocol._
        SprayJson
          .decode[SearchForeverRequest](jv)
          .liftErr(InvalidUserInput)
          .map(Query(_, this))
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
    : StreamQueryReader[NonEmptyList[domain.ContractKeyStreamRequest[None.type, LfV]]] =
    new StreamQueryReader[NonEmptyList[domain.ContractKeyStreamRequest[None.type, LfV]]] {

      private type CKR[+V] = domain.ContractKeyStreamRequest[None.type, V]

      import JsonProtocol._

      @SuppressWarnings(Array("org.wartremover.warts.Any"))
      override def parse(decoder: DomainJsonDecoder, jv: JsValue) =
        for {
          as <- SprayJson
            .decode[NonEmptyList[CKR[JsValue]]](jv)
            .liftErr(InvalidUserInput)
          bs = as.map(a => decodeWithFallback(decoder, a))
        } yield Query(bs, new EnrichedContractKeyWithStreamQuery[None.type])

      private def decodeWithFallback(decoder: DomainJsonDecoder, a: CKR[JsValue]): CKR[LfV] =
        decoder
          .decodeUnderlyingValuesToLf(a)
          .valueOr(_ => a.map(_ => com.digitalasset.daml.lf.value.Value.ValueUnit)) // unit will not match any key

    }

  private[this] final class EnrichedContractKeyWithStreamQuery[Off]
      extends StreamQuery[NonEmptyList[domain.ContractKeyStreamRequest[Off, LfV]]] {
    type Positive = Unit

    private type CKR[+V] = domain.ContractKeyStreamRequest[Off, V]

    override def allowPhantonArchives: Boolean = false

    override def predicate(
        request: NonEmptyList[CKR[LfV]],
        resolveTemplateId: PackageService.ResolveTemplateId,
        lookupType: TypeLookup): StreamPredicate[Positive] = {

      import util.Collections._

      val (resolvedWithKey, unresolved) =
        request.toSet.partitionMap { x: CKR[LfV] =>
          resolveTemplateId(x.ekey.templateId)
            .map((_, x.ekey.key))
            .toLeftDisjunction(x.ekey.templateId)
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
  import Statement.discard
  import util.ErrorOps._
  import com.digitalasset.http.json.JsonProtocol._

  private val config = wsConfig.getOrElse(Config.DefaultWsConfig)

  private val numConns = new java.util.concurrent.atomic.AtomicInteger(0)

  private[http] def transactionMessageHandler[A: StreamQueryReader](
      jwt: Jwt,
      jwtPayload: JwtPayload,
  ): Flow[Message, Message, _] =
    wsMessageHandler[A](jwt, jwtPayload)
      .via(applyConfig)
      .via(connCounter)

  private def applyConfig[A]: Flow[A, A, NotUsed] =
    Flow[A]
      .takeWithin(config.maxDuration)
      .throttle(config.throttleElem, config.throttlePer, config.maxBurst, config.mode)

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

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def wsMessageHandler[A: StreamQueryReader](
      jwt: Jwt,
      jwtPayload: JwtPayload,
  ): Flow[Message, Message, NotUsed] = {
    val Q = implicitly[StreamQueryReader[A]]
    Flow[Message]
      .mapAsync(1) {
        case msg: TextMessage =>
          msg.toStrict(config.maxDuration).map { m =>
            SprayJson.parse(m.text).liftErr(InvalidUserInput)
          }
        case bm: BinaryMessage =>
          // ignore binary messages but drain content to avoid the stream being clogged
          discard { bm.dataStream.runWith(Sink.ignore) }
          Future successful -\/(InvalidUserInput(
            "Invalid request. Expected a single TextMessage with JSON payload, got BinaryMessage"))
      }
      .via(withOptPrefix(ejv => ejv.toOption flatMap readStartingOffset))
      .map {
        case (oeso, ejv) =>
          for {
            offPrefix <- oeso.sequence
            jv <- ejv
            a <- Q.parse(decoder, jv)
          } yield (offPrefix, a)
      }
      .map {
        _.flatMap {
          case (offPrefix, qq: Q.Query[q]) =>
            implicit val SQ: StreamQuery[q] = qq.alg
            getTransactionSourceForParty[q](jwt, jwtPayload, offPrefix, qq.q: q)
        }
      }
      .takeWhile(_.isRight, inclusive = true) // stop after emitting 1st error
      .flatMapConcat {
        case \/-(s) => s
        case -\/(e) => Source.single(wsErrorMessage(e))
      }
  }

  private def getTransactionSourceForParty[A: StreamQuery](
      jwt: Jwt,
      jwtPayload: JwtPayload,
      offPrefix: Option[domain.StartingOffset],
      request: A): Error \/ Source[Message, NotUsed] = {
    val Q = implicitly[StreamQuery[A]]

    val (resolved, unresolved, fn) = Q.predicate(request, resolveTemplateId, lookupType)

    if (resolved.nonEmpty) {
      val source = contractsService
        .insertDeleteStepSource(jwt, jwtPayload.party, resolved.toList, offPrefix, Terminates.Never)
        .via(convertFilterContracts(fn))
        .filter(_.nonEmpty)
        .via(removePhantomArchives(remove = !Q.allowPhantonArchives))
        .map(_.mapPos(Q.renderCreatedMetadata).render)
        .via(renderEventsAndEmitHeartbeats) // wrong place, see https://github.com/digital-asset/daml/issues/4955
        .prepend(reportUnresolvedTemplateIds(unresolved))
        .map(jsv => TextMessage(jsv.compactPrint))
      \/-(source)
    } else {
      -\/(InvalidUserInput(
        s"Cannot resolve any of the requested template IDs: ${unresolved: Set[domain.TemplateId.OptionalPkg]}"))
    }
  }

  // TODO(Leo): #4955 make sure the heartbeat contains the last seen offset,
  // including the offset from the last empty/filtered out block of events
  private def renderEventsAndEmitHeartbeats: Flow[EventsAndOffset, JsValue, NotUsed] =
    Flow[EventsAndOffset]
      .keepAlive(config.heartBeatPer, () => (Vector.empty[JsObject], Option.empty[JsValue])) // heartbeat message
      .scan(Option.empty[(EventsAndOffset, EventsAndOffset)]) {
        case (Some((s, _)), (Vector(), None)) =>
          // heartbeat message, keep the previous state, report the last seen offset
          Some((s, (Vector(), s._2)))
        case (_, a) =>
          // override state, capture the last seen offset
          Some((a, a))
      }
      .collect {
        case Some((_, a)) => renderEvents(a._1, a._2)
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

  private[http] def wsErrorMessage(error: Error): TextMessage.Strict = {
    TextMessage(errorResponse(error).toJson.compactPrint)
  }

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
