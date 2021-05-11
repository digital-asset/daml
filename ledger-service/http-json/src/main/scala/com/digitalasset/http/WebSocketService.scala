// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.Materializer
import com.daml.http.EndpointsCompanion._
import com.daml.http.domain.{JwtPayload, SearchForeverRequest}
import com.daml.http.json.{DomainJsonDecoder, JsonProtocol, SprayJson}
import JsonProtocol.LfValueDatabaseCodec
import com.daml.http.LedgerClientJwt.Terminates
import util.ApiValueToLfValueConverter.apiValueToLfValue
import util.{BeginBookmark, ContractStreamStep, InsertDeleteStep}
import ContractStreamStep.LiveBegin
import json.JsonProtocol.LfValueCodec.{apiValueToJsValue => lfValueToJsValue}
import query.ValuePredicate.{LfV, TypeLookup}
import com.daml.jwt.domain.Jwt
import com.daml.http.query.ValuePredicate
import doobie.ConnectionIO
import doobie.syntax.string._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.std.map._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.std.vector._
import scalaz.{-\/, Foldable, Liskov, NonEmptyList, OneAnd, Tag, \/, \/-}
import Liskov.<~<
import com.daml.http.util.FlowUtil.allowOnlyFirstInput
import com.daml.http.util.Logging.{CorrelationID, RequestID}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import spray.json.{JsArray, JsObject, JsValue, JsonReader}

import scala.collection.compat._
import scala.collection.mutable.HashSet
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object WebSocketService {
  import util.ErrorOps._

  private type CompiledQueries =
    Map[domain.TemplateId.RequiredPkg, (ValuePredicate, LfV => Boolean)]

  private final case class StreamPredicate[+Positive](
      resolved: Set[domain.TemplateId.RequiredPkg],
      unresolved: Set[domain.TemplateId.OptionalPkg],
      fn: domain.ActiveContract[LfV] => Option[Positive],
      dbQuery: (OneAnd[Set, domain.Party], dbbackend.ContractDao) => ConnectionIO[
        _ <: Vector[(domain.ActiveContract[JsValue], Positive)]
      ],
  )

  /** If an element satisfies `prefix`, consume it and emit the result alongside
    * the next element (which is not similarly tested); otherwise, emit it.
    *
    * {{{
    *   withOptPrefix(_ => None)
    *     = Flow[I].map((None, _))
    *
    *   Source(Seq(1, 2, 3, 4)) via withOptPrefix(Some(_))
    *     = Source(Seq((Some(1), 2), (Some(3), 4)))
    * }}}
    */
  private def withOptPrefix[I, L](prefix: I => Option[L]): Flow[I, (Option[L], I), NotUsed] =
    Flow[I]
      .scan(none[L \/ (Option[L], I)]) { (s, i) =>
        s match {
          case Some(-\/(l)) => Some(\/-((some(l), i)))
          case None | Some(\/-(_)) => Some(prefix(i) toLeftDisjunction ((none, i)))
        }
      }
      .collect { case Some(\/-(oli)) => oli }

  private final case class StepAndErrors[+Pos, +LfVT](
      errors: Seq[ServerError],
      step: ContractStreamStep[domain.ArchivedContract, (domain.ActiveContract[LfVT], Pos)],
  ) {
    import JsonProtocol._, spray.json._

    def render(implicit lfv: LfVT <~< JsValue, pos: Pos <~< Map[String, JsValue]): JsObject = {

      def inj[V: JsonWriter](ctor: String, v: V) = JsObject(ctor -> v.toJson)

      val InsertDeleteStep(inserts, deletes) =
        Liskov
          .lift2[StepAndErrors, Pos, Map[String, JsValue], LfVT, JsValue](pos, lfv)(this)
          .step
          .toInsertDelete

      val events = (deletes.valuesIterator.map(inj("archived", _)).toVector
        ++ inserts.map { case (ac, pos) =>
          val acj = inj("created", ac)
          acj copy (fields = acj.fields ++ pos)
        } ++ errors.map(e => inj("error", e.message)))

      val offsetAfter = step.bookmark.map(_.toJson)

      renderEvents(events, offsetAfter)
    }

    def append[P >: Pos, A >: LfVT](o: StepAndErrors[P, A]): StepAndErrors[P, A] =
      StepAndErrors(errors ++ o.errors, step append o.step)

    def mapLfv[A](f: LfVT => A): StepAndErrors[Pos, A] =
      copy(step = step mapPreservingIds (_ leftMap (_ map f)))

    def mapPos[P](f: Pos => P): StepAndErrors[P, LfVT] =
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
          if (fields.sizeIs > 1)
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
        identity,
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

    private[WebSocketService] def predicate(
        request: A,
        resolveTemplateId: PackageService.ResolveTemplateId,
        lookupType: ValuePredicate.TypeLookup,
    ): StreamPredicate[Positive]

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

      override private[WebSocketService] def predicate(
          request: SearchForeverRequest,
          resolveTemplateId: PackageService.ResolveTemplateId,
          lookupType: ValuePredicate.TypeLookup,
      ): StreamPredicate[Positive] = {

        import scalaz.syntax.foldable._
        import util.Collections._

        val (resolved, unresolved, q) = request.queries.zipWithIndex
          .foldMap { case (gacr, ix) =>
            val (resolved, unresolved) =
              gacr.templateIds.toSet.partitionMap(x => resolveTemplateId(x).toLeft(x))
            val q: CompiledQueries = prepareFilters(resolved, gacr.query, lookupType)
            (resolved, unresolved, q transform ((_, p) => NonEmptyList((p, ix))))
          }
        def fn(a: domain.ActiveContract[LfV]): Option[Positive] =
          q.get(a.templateId).flatMap { preds =>
            preds.collect(Function unlift { case ((_, p), ix) => p(a.payload) option ix })
          }
        def dbQueriesPlan(implicit
            sjd: dbbackend.SupportedJdbcDriver
        ): (Seq[(domain.TemplateId.RequiredPkg, doobie.Fragment)], Map[Int, Int]) = {
          val annotated = q.toSeq.flatMap { case (tpid, nel) =>
            nel.toVector.map { case ((vp, _), pos) => (tpid, vp.toSqlWhereClause, pos) }
          }
          val posMap = annotated.iterator.zipWithIndex.map { case ((_, _, pos), ix) =>
            (ix, pos)
          }.toMap
          (annotated map { case (tpid, sql, _) => (tpid, sql) }, posMap)
        }

        StreamPredicate(
          resolved,
          unresolved,
          fn,
          { (parties, dao) =>
            import dao.{logHandler, jdbcDriver}
            import dbbackend.ContractDao.{selectContractsMultiTemplate, MatchedQueryMarker}
            val (dbQueries, posMap) = dbQueriesPlan
            selectContractsMultiTemplate(parties, dbQueries, MatchedQueryMarker.ByNelInt)
              .map(_ map (_ rightMap (_ map posMap)))
          },
        )
      }

      private def prepareFilters(
          ids: Set[domain.TemplateId.RequiredPkg],
          queryExpr: Map[String, JsValue],
          lookupType: ValuePredicate.TypeLookup,
      ): CompiledQueries =
        ids.iterator.map { tid =>
          val vp = ValuePredicate.fromTemplateJsObject(queryExpr, tid, lookupType)
          (tid, (vp, vp.toFunPredicate))
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

      override def parse(resumingAtOffset: Boolean, decoder: DomainJsonDecoder, jv: JsValue) = {
        type NelCKRH[Hint, V] = NonEmptyList[domain.ContractKeyStreamRequest[Hint, V]]
        def go[Hint](
            alg: StreamQuery[NelCKRH[Hint, LfV]]
        )(implicit ev: JsonReader[NelCKRH[Hint, JsValue]]) =
          for {
            as <- SprayJson
              .decode[NelCKRH[Hint, JsValue]](jv)
              .liftErr(InvalidUserInput)
            bs = as.map(a => decodeWithFallback(decoder, a))
          } yield Query(bs, alg)
        if (resumingAtOffset) go(ResumingEnrichedContractKeyWithStreamQuery)
        else go(InitialEnrichedContractKeyWithStreamQuery)
      }

      private def decodeWithFallback[Hint](
          decoder: DomainJsonDecoder,
          a: domain.ContractKeyStreamRequest[Hint, JsValue],
      ) =
        decoder
          .decodeUnderlyingValuesToLf(a)
          .valueOr(_ =>
            a.map(_ => com.daml.lf.value.Value.ValueUnit)
          ) // unit will not match any key

    }

  private[this] sealed abstract class EnrichedContractKeyWithStreamQuery[Cid]
      extends StreamQuery[NonEmptyList[domain.ContractKeyStreamRequest[Cid, LfV]]] {
    type Positive = Unit

    protected type CKR[+V] = domain.ContractKeyStreamRequest[Cid, V]

    override private[WebSocketService] def predicate(
        request: NonEmptyList[CKR[LfV]],
        resolveTemplateId: PackageService.ResolveTemplateId,
        lookupType: TypeLookup,
    ): StreamPredicate[Positive] = {
      val (resolvedWithKey, unresolved) =
        request.toSet.partitionMap { x: CKR[LfV] =>
          resolveTemplateId(x.ekey.templateId)
            .map((_, x.ekey.key))
            .toLeft(x.ekey.templateId)
        }

      // invariant: every set is non-empty
      val q: Map[domain.TemplateId.RequiredPkg, HashSet[LfV]] =
        resolvedWithKey.foldLeft(Map.empty[domain.TemplateId.RequiredPkg, HashSet[LfV]])(
          (acc, el) =>
            acc.get(el._1) match {
              case Some(v) => acc.updated(el._1, v += el._2)
              case None => acc.updated(el._1, HashSet(el._2))
            }
        )
      val fn: domain.ActiveContract[LfV] => Option[Positive] = { a =>
        a.key match {
          case None => None
          case Some(k) => if (q.getOrElse(a.templateId, HashSet()).contains(k)) Some(()) else None
        }
      }
      def dbQueries(implicit
          sjd: dbbackend.SupportedJdbcDriver
      ): Seq[(domain.TemplateId.RequiredPkg, doobie.Fragment)] =
        q.toSeq map (_ rightMap { lfvKeys =>
          val khd +: ktl = lfvKeys.toVector
          import dbbackend.Queries.{intersperse, concatFragment}
          concatFragment(intersperse(OneAnd(khd, ktl) map (keyEquality(_)), sql" OR "))
        })
      StreamPredicate(
        q.keySet,
        unresolved,
        fn,
        { (parties, dao) =>
          import dao.{logHandler, jdbcDriver}
          import dbbackend.ContractDao.{selectContractsMultiTemplate, MatchedQueryMarker}
          selectContractsMultiTemplate(parties, dbQueries, MatchedQueryMarker.Unused)
        },
      )
    }

    override def renderCreatedMetadata(p: Unit) = Map.empty
  }

  private[this] def keyEquality(k: LfV)(implicit
      sjd: dbbackend.SupportedJdbcDriver
  ): doobie.Fragment =
    sjd.queries.keyEquality(LfValueDatabaseCodec.apiValueToJsValue(k))

  private[this] object InitialEnrichedContractKeyWithStreamQuery
      extends EnrichedContractKeyWithStreamQuery[Unit] {
    override def removePhantomArchives(request: NonEmptyList[CKR[LfV]]) = Some(Set.empty)
  }

  private[this] object ResumingEnrichedContractKeyWithStreamQuery
      extends EnrichedContractKeyWithStreamQuery[Option[Option[domain.ContractId]]] {
    override def removePhantomArchives(request: NonEmptyList[CKR[LfV]]) = {
      val NelO = Foldable[NonEmptyList].compose[Option]
      request traverse (_.contractIdAtOffset) map NelO.toSet
    }
  }

  private abstract sealed class TickTriggerOrStep[+A] extends Product with Serializable
  private final case object TickTrigger extends TickTriggerOrStep[Nothing]
  private final case class Step[A](payload: StepAndErrors[A, JsValue]) extends TickTriggerOrStep[A]
}

class WebSocketService(
    contractsService: ContractsService,
    resolveTemplateId: PackageService.ResolveTemplateId,
    decoder: DomainJsonDecoder,
    lookupType: ValuePredicate.TypeLookup,
    wsConfig: Option[WebsocketConfig],
)(implicit mat: Materializer, ec: ExecutionContext) {

  private[this] val logger = ContextualizedLogger.get(getClass)

  import WebSocketService._
  import com.daml.scalautil.Statement.discard
  import util.ErrorOps._
  import com.daml.http.json.JsonProtocol._

  private val config = wsConfig.getOrElse(Config.DefaultWsConfig)

  private val numConns = new java.util.concurrent.atomic.AtomicInteger(0)

  private[http] def transactionMessageHandler[A: StreamQueryReader](
      jwt: Jwt,
      jwtPayload: JwtPayload,
  )(implicit lc: LoggingContextOf[CorrelationID with RequestID]): Flow[Message, Message, _] =
    wsMessageHandler[A](jwt, jwtPayload)
      .via(applyConfig)
      .via(connCounter)

  private def applyConfig[A]: Flow[A, A, NotUsed] =
    Flow[A]
      .takeWithin(config.maxDuration)
      .throttle(config.throttleElem, config.throttlePer, config.maxBurst, config.mode)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.NonUnitStatements",
      "org.wartremover.warts.JavaSerializable",
      "org.wartremover.warts.Serializable",
    )
  )
  private def connCounter[A](implicit
      lc: LoggingContextOf[CorrelationID with RequestID]
  ): Flow[A, A, NotUsed] =
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
              s"Websocket client interrupted on Failure: ${ex.getMessage}. remaining number of clients: $numConns"
            )
        }
        NotUsed
      }

  private def wsMessageHandler[A: StreamQueryReader](
      jwt: Jwt,
      jwtPayload: JwtPayload,
  )(implicit
      lc: LoggingContextOf[CorrelationID with RequestID]
  ): Flow[Message, Message, NotUsed] = {
    val Q = implicitly[StreamQueryReader[A]]
    Flow[Message]
      .mapAsync(1)(parseJson)
      .via(withOptPrefix(ejv => ejv.toOption flatMap readStartingOffset))
      .map { case (oeso, ejv) =>
        for {
          offPrefix <- oeso.sequence
          jv <- ejv
          a <- Q.parse(resumingAtOffset = offPrefix.isDefined, decoder, jv)
        } yield (offPrefix, a)
      }
      .via(
        allowOnlyFirstInput(
          InvalidUserInput("Multiple requests over the same WebSocket connection are not allowed.")
        )
      )
      .flatMapMerge(
        2, // 2 streams max, the 2nd is to be able to send an error back
        _.map { case (offPrefix, qq: Q.Query[q]) =>
          implicit val SQ: StreamQuery[q] = qq.alg
          getTransactionSourceForParty[q](jwt, jwtPayload.parties, offPrefix, qq.q: q)
        }.valueOr(e => Source.single(-\/(e))): Source[Error \/ Message, NotUsed],
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
          "Invalid request. Expected a single TextMessage with JSON payload, got BinaryMessage"
        )
      )
  }

  private def getTransactionSourceForParty[A: StreamQuery](
      jwt: Jwt,
      parties: OneAnd[Set, domain.Party],
      offPrefix: Option[domain.StartingOffset],
      request: A,
  )(implicit
      lc: LoggingContextOf[CorrelationID with RequestID]
  ): Source[Error \/ Message, NotUsed] = {
    val Q = implicitly[StreamQuery[A]]

    val StreamPredicate(resolved, unresolved, fn, dbQuery) =
      Q.predicate(request, resolveTemplateId, lookupType)

    if (resolved.nonEmpty) {
      def prefilter: Future[
        (
            Source[
              ContractStreamStep[Nothing, (domain.ActiveContract[JsValue], Q.Positive)],
              NotUsed,
            ],
            Option[domain.StartingOffset],
        )
      ] =
        contractsService.daoAndFetch
          .filter(_ => offPrefix.isEmpty)
          .cata(
            { case (dao, fetch) =>
              val tx = for {
                bookmark <- fetch.fetchAndPersist(jwt, parties, resolved.toList)
                mdContracts <- dbQuery(parties, dao)
              } yield (
                Source.single(mdContracts).map(ContractStreamStep.Acs(_)) ++ Source
                  .single(ContractStreamStep.LiveBegin(bookmark.map(_.toDomain))),
                bookmark.toOption map (term => domain.StartingOffset(term.toDomain)),
              )
              dao.transact(tx).unsafeToFuture()
            },
            Future.successful((Source.empty, offPrefix)),
          )

      Source
        .lazyFutureSource(() =>
          prefilter.map { case (prefiltered, shiftedPrefix) =>
            val liveFiltered = contractsService
              .insertDeleteStepSource(
                jwt,
                parties,
                resolved.toList,
                shiftedPrefix,
                Terminates.Never,
              )
              .via(convertFilterContracts(fn))
            (prefiltered.map(StepAndErrors(Seq.empty, _)) ++ liveFiltered)
              .via(emitOffsetTicksAndFilterOutEmptySteps(shiftedPrefix))
          }
        )
        .mapMaterializedValue { _: Future[NotUsed] =>
          NotUsed
        }
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

  private def emitOffsetTicksAndFilterOutEmptySteps[Pos](
      offset: Option[domain.StartingOffset]
  ): Flow[StepAndErrors[Pos, JsValue], StepAndErrors[Pos, JsValue], NotUsed] = {

    val zero = (
      offset.map(o => util.AbsoluteBookmark(o.offset)): Option[BeginBookmark[domain.Offset]],
      TickTrigger: TickTriggerOrStep[Pos],
    )

    Flow[StepAndErrors[Pos, JsValue]]
      .map(a => Step(a))
      .keepAlive(config.heartBeatPer, () => TickTrigger)
      .scan(zero) {
        case ((None, _), TickTrigger) =>
          // skip all ticks we don't have the offset yet
          (None, TickTrigger)
        case ((Some(offset), _), TickTrigger) =>
          // emit an offset tick
          (Some(offset), Step(offsetTick(offset)))
        case ((_, _), msg @ Step(_)) =>
          // capture the new offset and emit the current step
          val newOffset = msg.payload.step.bookmark
          (newOffset, msg)
      }
      // filter non-empty Steps, we don't want to spam client with empty events
      .collect { case (_, Step(x)) if x.nonEmpty => x }
  }

  private def offsetTick[Pos](offset: BeginBookmark[domain.Offset]) =
    StepAndErrors[Pos, JsValue](Seq.empty, LiveBegin(offset))

  private def removePhantomArchives[A, B](remove: Option[Set[domain.ContractId]]) =
    remove.cata(removePhantomArchives_[A, B], Flow[StepAndErrors[A, B]])

  private def removePhantomArchives_[A, B](
      initialState: Set[domain.ContractId]
  ): Flow[StepAndErrors[A, B], StepAndErrors[A, B], NotUsed] = {
    import ContractStreamStep.{LiveBegin, Txn, Acs}
    Flow[StepAndErrors[A, B]]
      .scan((Tag unsubst initialState, Option.empty[StepAndErrors[A, B]])) {
        case ((s0, _), a0 @ StepAndErrors(_, Txn(idstep, _))) =>
          val newInserts: Vector[String] =
            domain.ContractId.unsubst(idstep.inserts.map(_._1.contractId))
          val (deletesToEmit, deletesToHold) = s0 partition idstep.deletes.keySet
          val s1: Set[String] = deletesToHold ++ newInserts
          val a1 = a0.copy(step = a0.step.mapDeletes(_.view.filterKeys(deletesToEmit).toMap))

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

  private def convertFilterContracts[Pos](
      fn: domain.ActiveContract[LfV] => Option[Pos]
  ): Flow[ContractStreamStep.LAV1, StepAndErrors[Pos, JsValue], NotUsed] =
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
        )(implicitly[Factory[ServerError, Seq[ServerError]]])
        StepAndErrors(
          errors ++ aerrors,
          dstep mapInserts { inserts: Vector[domain.ActiveContract[LfV]] =>
            inserts.flatMap { ac =>
              fn(ac).map((ac, _)).toList
            }
          },
        )
      }
      .via(conflation)
      .map(_ mapLfv lfValueToJsValue)

  private def reportUnresolvedTemplateIds(
      unresolved: Set[domain.TemplateId.OptionalPkg]
  ): Source[JsValue, NotUsed] =
    if (unresolved.isEmpty) Source.empty
    else {
      import spray.json._
      Source.single(
        domain.AsyncWarningsWrapper(domain.UnknownTemplateIds(unresolved.toList)).toJson
      )
    }
}
