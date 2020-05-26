// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.model.ws.{Message, TextMessage, BinaryMessage}
import akka.stream.scaladsl.{Flow, Source, Sink}
import akka.stream.Materializer
import com.daml.http.EndpointsCompanion._
import com.daml.http.domain.{JwtPayload, SearchForeverRequest}
import com.daml.http.json.{DomainJsonDecoder, DomainJsonEncoder, JsonProtocol, SprayJson}
import com.daml.http.LedgerClientJwt.Terminates
import util.ApiValueToLfValueConverter.apiValueToLfValue
import util.{AbsoluteBookmark, ContractStreamStep, InsertDeleteStep, LedgerBegin}
import ContractStreamStep.{Acs, LiveBegin, Txn}
import json.JsonProtocol.LfValueCodec.{apiValueToJsValue => lfValueToJsValue}
import query.ValuePredicate.{LfV, TypeLookup}
import com.daml.jwt.domain.Jwt
import com.typesafe.scalalogging.LazyLogging
import com.daml.http.query.ValuePredicate
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.std.map._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.{-\/, Foldable, Liskov, NonEmptyList, Tag, \/, \/-}
import Liskov.<~<
import com.daml.http.util.FlowUtil.allowOnlyFirstInput
import spray.json.{JsArray, JsObject, JsValue, JsonReader}

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
    import JsonProtocol._, spray.json._

    def render(implicit lfv: LfV <~< JsValue, pos: Pos <~< Map[String, JsValue]): JsObject = {

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

      val offsetAfter = step.bookmark.map(_.toJson)

      renderEvents(events, offsetAfter)
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
              .decode[domain.Offset](offJv)
              .liftErr(InvalidUserInput)
              .map(offset => domain.StartingOffset(offset))
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
    def parse(resumingAtOffset: Boolean, decoder: DomainJsonDecoder, jv: JsValue): Error \/ Query[_]
  }

  sealed trait StreamQuery[A] {

    /** Extra data on success of a predicate. */
    type Positive

    def removePhantomArchives(request: A): Option[Set[domain.ContractId]]

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

      override def parse(resumingAtOffset: Boolean, decoder: DomainJsonDecoder, jv: JsValue) = {
        import JsonProtocol._
        SprayJson
          .decode[SearchForeverRequest](jv)
          .liftErr(InvalidUserInput)
          .map(Query(_, this))
      }

      override def removePhantomArchives(request: SearchForeverRequest) = None

      @SuppressWarnings(Array("org.wartremover.warts.Any"))
      override def predicate(
          request: SearchForeverRequest,
          resolveTemplateId: PackageService.ResolveTemplateId,
          lookupType: ValuePredicate.TypeLookup): StreamPredicate[Positive] = {

        import scalaz.syntax.foldable._
        import util.Collections._

        val (resolved, unresolved, q) = request.queries.zipWithIndex
          .foldMap {
            case (gacr, ix) =>
              val (resolved, unresolved) =
                gacr.templateIds.toSet.partitionMap(x => resolveTemplateId(x) toLeftDisjunction x)
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
    : StreamQueryReader[domain.ContractKeyStreamRequest[_, _]] =
    new StreamQueryReader[domain.ContractKeyStreamRequest[_, _]] {

      import JsonProtocol._

      @SuppressWarnings(Array("org.wartremover.warts.Any"))
      override def parse(resumingAtOffset: Boolean, decoder: DomainJsonDecoder, jv: JsValue) = {
        type NelCKRH[Hint, V] = NonEmptyList[domain.ContractKeyStreamRequest[Hint, V]]
        def go[Hint](alg: StreamQuery[NelCKRH[Hint, LfV]])(
            implicit ev: JsonReader[NelCKRH[Hint, JsValue]]) =
          for {
            as <- SprayJson
              .decode[NelCKRH[Hint, JsValue]](jv)
              .liftErr(InvalidUserInput)
            bs = as.map(a => decodeWithFallback(decoder, a))
          } yield Query(bs, alg)
        if (resumingAtOffset) go(ResumingEnrichedContractKeyWithStreamQuery)
        else go(InitialEnrichedContractKeyWithStreamQuery)
      }

      @SuppressWarnings(Array("org.wartremover.warts.Any"))
      private def decodeWithFallback[Hint](
          decoder: DomainJsonDecoder,
          a: domain.ContractKeyStreamRequest[Hint, JsValue]) =
        decoder
          .decodeUnderlyingValuesToLf(a)
          .valueOr(_ => a.map(_ => com.daml.lf.value.Value.ValueUnit)) // unit will not match any key

    }

  private[this] sealed abstract class EnrichedContractKeyWithStreamQuery[Cid]
      extends StreamQuery[NonEmptyList[domain.ContractKeyStreamRequest[Cid, LfV]]] {
    type Positive = Unit

    protected type CKR[+V] = domain.ContractKeyStreamRequest[Cid, V]

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

  private[this] object InitialEnrichedContractKeyWithStreamQuery
      extends EnrichedContractKeyWithStreamQuery[Unit] {
    override def removePhantomArchives(request: NonEmptyList[CKR[LfV]]) = Some(Set.empty)
  }

  private[this] object ResumingEnrichedContractKeyWithStreamQuery
      extends EnrichedContractKeyWithStreamQuery[Option[Option[domain.ContractId]]] {
    override def removePhantomArchives(request: NonEmptyList[CKR[LfV]]) = {
      @SuppressWarnings(Array("org.wartremover.warts.Any"))
      val NelO = Foldable[NonEmptyList].compose[Option]
      request traverse (_.contractIdAtOffset) map NelO.toSet
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
  import Statement.discard
  import util.ErrorOps._
  import com.daml.http.json.JsonProtocol._

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
      .mapAsync(1)(parseJson)
      .via(withOptPrefix(ejv => ejv.toOption flatMap readStartingOffset))
      .map {
        case (oeso, ejv) =>
          for {
            offPrefix <- oeso.sequence
            jv <- ejv
            a <- Q.parse(resumingAtOffset = offPrefix.isDefined, decoder, jv)
          } yield (offPrefix, a)
      }
      .via(allowOnlyFirstInput(
        InvalidUserInput("Multiple requests over the same WebSocket connection are not allowed.")))
      .flatMapMerge(
        2, // 2 streams max, the 2nd is to be able to send an error back
        _.map {
          case (offPrefix, qq: Q.Query[q]) =>
            implicit val SQ: StreamQuery[q] = qq.alg
            getTransactionSourceForParty[q](jwt, jwtPayload.party, offPrefix, qq.q: q)
        }.valueOr(e => Source.single(-\/(e))): Source[Error \/ Message, NotUsed]
      )
      .takeWhile(_.isRight, inclusive = true) // stop after emitting 1st error
      .map(_.fold(e => wsErrorMessage(e), identity): Message)
  }

  private def parseJson(x: Message): Future[InvalidUserInput \/ JsValue] = x match {
    case msg: TextMessage =>
      msg.toStrict(config.maxDuration).map { m =>
        SprayJson.parse(m.text).liftErr(InvalidUserInput)
      }
    case bm: BinaryMessage =>
      // ignore binary messages but drain content to avoid the stream being clogged
      discard { bm.dataStream.runWith(Sink.ignore) }
      Future successful -\/(
        InvalidUserInput(
          "Invalid request. Expected a single TextMessage with JSON payload, got BinaryMessage"))
  }

  private def getTransactionSourceForParty[A: StreamQuery](
      jwt: Jwt,
      party: domain.Party,
      offPrefix: Option[domain.StartingOffset],
      request: A): Source[Error \/ Message, NotUsed] = {
    val Q = implicitly[StreamQuery[A]]

    val (resolved, unresolved, fn) = Q.predicate(request, resolveTemplateId, lookupType)

    if (resolved.nonEmpty) {
      contractsService
        .insertDeleteStepSource(jwt, party, resolved.toList, offPrefix, Terminates.Never)
        .via(convertFilterContracts(fn))
        .via(emitOffsetTicksAndFilterOutEmptySteps(offPrefix))
        .via(removePhantomArchives(remove = Q.removePhantomArchives(request)))
        .map(_.mapPos(Q.renderCreatedMetadata).render)
        .prepend(reportUnresolvedTemplateIds(unresolved))
        .map(jsv => \/-(wsMessage(jsv)))
    } else {
      reportUnresolvedTemplateIds(unresolved)
        .map(jsv => \/-(wsMessage(jsv)))
        .concat(Source.single(-\/(InvalidUserInput(ErrorMessages.cannotResolveAnyTemplateId))))
    }
  }

  private def emitOffsetTicksAndFilterOutEmptySteps[Pos](startFrom: Option[domain.StartingOffset])
    : Flow[StepAndErrors[Pos, JsValue], StepAndErrors[Pos, JsValue], NotUsed] = {

    type TickTriggerOrStep = Unit \/ StepAndErrors[Pos, JsValue]

    val tickTrigger: TickTriggerOrStep = -\/(())
    val zeroState: StepAndErrors[Pos, JsValue] = startFrom.cata(
      x => StepAndErrors(Seq(), LiveBegin(AbsoluteBookmark(x.offset))),
      StepAndErrors(Seq(), LiveBegin(LedgerBegin))
    )
    Flow[StepAndErrors[Pos, JsValue]]
      .map(a => \/-(a): TickTriggerOrStep)
      .keepAlive(config.heartBeatPer, () => tickTrigger)
      .scan((zeroState, tickTrigger)) {
        case ((state, _), -\/(())) =>
          // convert tick trigger into a tick message, get the last seen offset from the state
          state.step match {
            case Acs(_) => (ledgerBeginTick, \/-(ledgerBeginTick))
            case LiveBegin(LedgerBegin) => (ledgerBeginTick, \/-(ledgerBeginTick))
            case LiveBegin(AbsoluteBookmark(offset)) => (state, \/-(offsetTick(offset)))
            case Txn(_, offset) => (state, \/-(offsetTick(offset)))
          }
        case ((_, _), x @ \/-(step)) =>
          // filter out empty steps, capture the current step, so we keep the last seen offset for the next tick
          val nonEmptyStep: TickTriggerOrStep = if (step.nonEmpty) x else tickTrigger
          (step, nonEmptyStep)
      }
      .collect { case (_, \/-(x)) => x }
  }

  private def ledgerBeginTick[Pos] =
    StepAndErrors[Pos, JsValue](Seq(), LiveBegin(LedgerBegin))

  private def offsetTick[Pos](offset: domain.Offset) =
    StepAndErrors[Pos, JsValue](Seq(), Txn(InsertDeleteStep.Empty, offset))

  private def removePhantomArchives[A, B](remove: Option[Set[domain.ContractId]]) =
    remove cata (removePhantomArchives_[A, B], Flow[StepAndErrors[A, B]])

  private def removePhantomArchives_[A, B](initialState: Set[domain.ContractId])
    : Flow[StepAndErrors[A, B], StepAndErrors[A, B], NotUsed] = {
    import ContractStreamStep.{LiveBegin, Txn, Acs}
    Flow[StepAndErrors[A, B]]
      .scan((Tag unsubst initialState, Option.empty[StepAndErrors[A, B]])) {
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

  private[http] def wsErrorMessage(error: Error): TextMessage.Strict =
    wsMessage(SprayJson.encodeUnsafe(errorResponse(error)))

  private[http] def wsMessage(jsVal: JsValue): TextMessage.Strict =
    TextMessage(jsVal.compactPrint)

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
    else {
      import spray.json._
      Source.single(
        domain.AsyncWarningsWrapper(domain.UnknownTemplateIds(unresolved.toList)).toJson)
    }
}
