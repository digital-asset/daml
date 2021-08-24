// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.Materializer
import com.daml.http.EndpointsCompanion._
import com.daml.http.domain.{JwtPayload, SearchForeverRequest, StartingOffset}
import com.daml.http.json.{DomainJsonDecoder, JsonProtocol, SprayJson}
import com.daml.http.LedgerClientJwt.Terminates
import util.ApiValueToLfValueConverter.apiValueToLfValue
import util.{BeginBookmark, ContractStreamStep, InsertDeleteStep}
import ContractStreamStep.{Acs, LiveBegin, Txn}
import json.JsonProtocol.LfValueCodec.{apiValueToJsValue => lfValueToJsValue}
import query.ValuePredicate.{LfV, TypeLookup}
import com.daml.jwt.domain.Jwt
import com.daml.http.query.ValuePredicate
import doobie.ConnectionIO
import doobie.syntax.string._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.Scalaz.futureInstance
import scalaz.std.map._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.std.vector._
import scalaz.syntax.traverse._
import scalaz.Scalaz.listInstance
import scalaz.{-\/, Foldable, Liskov, NonEmptyList, OneAnd, Tag, \/, \/-}
import Liskov.<~<
import com.daml.http.domain.TemplateId.toLedgerApiValue
import com.daml.http.domain.TemplateId.{OptionalPkg, RequiredPkg}
import com.daml.http.util.FlowUtil.allowOnlyFirstInput
import com.daml.http.util.Logging.InstanceUUID
import com.daml.lf.crypto.Hash
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.metrics.Metrics
import spray.json.{JsArray, JsObject, JsValue, JsonReader}

import scala.collection.compat._
import scala.collection.mutable.HashSet
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scalaz.EitherT.{either, eitherT, rightT}

object WebSocketService {
  import util.ErrorOps._

  private type CompiledQueries =
    Map[domain.TemplateId.RequiredPkg, (ValuePredicate, LfV => Boolean)]

  private final case class StreamPredicate[+Positive](
      resolved: Set[domain.TemplateId.RequiredPkg],
      unresolved: Set[domain.TemplateId.OptionalPkg],
      fn: (domain.ActiveContract[LfV], Option[domain.Offset]) => Option[Positive],
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
              .liftErr[Error](InvalidUserInput)
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
    def parse(
        resumingAtOffset: Boolean,
        decoder: DomainJsonDecoder,
        jv: JsValue,
    )(implicit
        lc: LoggingContextOf[InstanceUUID],
        jwt: Jwt,
    ): Future[Error \/ (_ <: Query[_])]
  }

  sealed trait StreamQuery[A] {

    /** Extra data on success of a predicate. */
    type Positive

    def removePhantomArchives(request: A): Option[Set[domain.ContractId]]

    private[WebSocketService] def predicate(
        request: A,
        resolveTemplateId: PackageService.ResolveTemplateId,
        lookupType: ValuePredicate.TypeLookup,
    )(implicit
        lc: LoggingContextOf[InstanceUUID],
        jwt: Jwt,
    ): Future[StreamPredicate[Positive]]

    def renderCreatedMetadata(p: Positive): Map[String, JsValue]

    def acsRequest(
        maybePrefix: Option[domain.StartingOffset],
        request: A,
    ): Option[A]

    /** Perform any necessary adjustment to the request based on the prefix
      */
    def adjustRequest(
        prefix: Option[domain.StartingOffset],
        request: A,
    ): A

    /** Specify the offset from which the live part of the query should start
      */
    def liveStartingOffset(
        prefix: Option[domain.StartingOffset],
        request: A,
    ): Option[domain.StartingOffset]

  }

  implicit def SearchForeverRequestWithStreamQuery(implicit
      ec: ExecutionContext
  ): StreamQueryReader[domain.SearchForeverRequest] =
    new StreamQueryReader[domain.SearchForeverRequest]
      with StreamQuery[domain.SearchForeverRequest] {

      type Positive = NonEmptyList[Int]

      override def parse(resumingAtOffset: Boolean, decoder: DomainJsonDecoder, jv: JsValue)(
          implicit
          lc: LoggingContextOf[InstanceUUID],
          jwt: Jwt,
      ) = {
        import JsonProtocol._
        Future.successful(
          SprayJson
            .decode[SearchForeverRequest](jv)
            .liftErr[Error](InvalidUserInput)
            .map(Query(_, this))
        )
      }

      override def removePhantomArchives(request: SearchForeverRequest) = None

      override private[WebSocketService] def predicate(
          request: SearchForeverRequest,
          resolveTemplateId: PackageService.ResolveTemplateId,
          lookupType: ValuePredicate.TypeLookup,
      )(implicit
          lc: LoggingContextOf[InstanceUUID],
          jwt: Jwt,
      ): Future[StreamPredicate[Positive]] = {

        import scalaz.syntax.foldable._
        import util.Collections._

        val indexedOffsets: Vector[Option[domain.Offset]] =
          request.queries.map(_.offset).toVector

        def matchesOffset(queryIndex: Int, maybeEventOffset: Option[domain.Offset]): Boolean = {
          import domain.Offset.ordering
          import scalaz.syntax.order._
          val matches =
            for {
              queryOffset <- indexedOffsets(queryIndex)
              eventOffset <- maybeEventOffset
            } yield eventOffset > queryOffset
          matches.getOrElse(true)
        }
        def fn(
            q: Map[RequiredPkg, NonEmptyList[((ValuePredicate, LfV => Boolean), Int)]]
        )(a: domain.ActiveContract[LfV], o: Option[domain.Offset]): Option[Positive] = {
          q.get(a.templateId).flatMap { preds =>
            preds.collect(Function unlift { case ((_, p), ix) =>
              val matchesPredicate = p(a.payload)
              (matchesPredicate && matchesOffset(ix, o)).option(ix)
            })
          }
        }

        def dbQueriesPlan(
            q: Map[RequiredPkg, NonEmptyList[((ValuePredicate, LfV => Boolean), Int)]]
        )(implicit
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
        for {
          res <-
            request.queries.zipWithIndex
              .foldMapM { case (gacr, ix) =>
                for {
                  res <-
                    gacr.templateIds.toList
                      .traverse(x =>
                        resolveTemplateId(lc)(jwt)(x).map(_.toOption.flatten.toLeft(x))
                      )
                      .map(
                        _.toSet[
                          Either[domain.TemplateId.RequiredPkg, domain.TemplateId.OptionalPkg]
                        ]
                          .partitionMap(identity)
                      )
                  (resolved, unresolved) = res
                  q = prepareFilters(resolved, gacr.query, lookupType): CompiledQueries
                } yield (resolved, unresolved, q transform ((_, p) => NonEmptyList((p, ix))))
              }
          (resolved, unresolved, q) = res
        } yield StreamPredicate(
          resolved,
          unresolved,
          fn(q),
          { (parties, dao) =>
            import dao.{logHandler, jdbcDriver}
            import dbbackend.ContractDao.{selectContractsMultiTemplate, MatchedQueryMarker}
            val (dbQueries, posMap) = dbQueriesPlan(q)
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

      override def acsRequest(
          maybePrefix: Option[domain.StartingOffset],
          request: SearchForeverRequest,
      ): Option[SearchForeverRequest] = {
        import scalaz.std.list
        val withoutOffset = request.queries.toList.filter(_.offset.isEmpty)
        list.toNel(withoutOffset).map(SearchForeverRequest(_))
      }

      override def adjustRequest(
          prefix: Option[domain.StartingOffset],
          request: SearchForeverRequest,
      ): SearchForeverRequest =
        prefix.fold(request)(prefix =>
          request.copy(
            queries = request.queries.map(query =>
              query.copy(offset = query.offset.orElse(Some(prefix.offset)))
            )
          )
        )

      import scalaz.syntax.foldable1._
      import domain.Offset.ordering
      import scalaz.std.option.optionOrder

      // This is called after `adjustRequest` already filled in the blank offsets
      override def liveStartingOffset(
          prefix: Option[domain.StartingOffset],
          request: SearchForeverRequest,
      ): Option[domain.StartingOffset] =
        request.queries
          .map(_.offset)
          .minimumBy1(identity)
          .map(domain.StartingOffset(_))

    }

  implicit def EnrichedContractKeyWithStreamQuery(implicit
      ec: ExecutionContext
  ): StreamQueryReader[domain.ContractKeyStreamRequest[_, _]] =
    new StreamQueryReader[domain.ContractKeyStreamRequest[_, _]] {

      import JsonProtocol._

      override def parse(resumingAtOffset: Boolean, decoder: DomainJsonDecoder, jv: JsValue)(
          implicit
          lc: LoggingContextOf[InstanceUUID],
          jwt: Jwt,
      ) = {
        import scalaz.Scalaz._
        type NelCKRH[Hint, V] = NonEmptyList[domain.ContractKeyStreamRequest[Hint, V]]
        def go[Hint](
            alg: StreamQuery[NelCKRH[Hint, LfV]]
        )(implicit ev: JsonReader[NelCKRH[Hint, JsValue]]) =
          for {
            as <- either[Future, Error, NelCKRH[Hint, JsValue]](
              SprayJson
                .decode[NelCKRH[Hint, JsValue]](jv)
                .liftErr[Error](InvalidUserInput)
            )
            bs <- rightT {
              as.map(a => decodeWithFallback(decoder, a)).sequence
            }
          } yield Query(bs, alg)
        if (resumingAtOffset) go(ResumingEnrichedContractKeyWithStreamQuery())
        else go(InitialEnrichedContractKeyWithStreamQuery())
      }.run

      private def decodeWithFallback[Hint](
          decoder: DomainJsonDecoder,
          a: domain.ContractKeyStreamRequest[Hint, JsValue],
      )(implicit
          lc: LoggingContextOf[InstanceUUID],
          jwt: Jwt,
      ): Future[domain.ContractKeyStreamRequest[Hint, domain.LfValue]] =
        decoder
          .decodeUnderlyingValuesToLf(a)
          .run
          .map(
            _.valueOr(_ => a.map(_ => com.daml.lf.value.Value.ValueUnit))
          ) // unit will not match any key

    }

  private[this] sealed abstract class EnrichedContractKeyWithStreamQuery[Cid](implicit
      val ec: ExecutionContext
  ) extends StreamQuery[NonEmptyList[domain.ContractKeyStreamRequest[Cid, LfV]]] {
    type Positive = Unit

    protected type CKR[+V] = domain.ContractKeyStreamRequest[Cid, V]

    override private[WebSocketService] def predicate(
        request: NonEmptyList[CKR[LfV]],
        resolveTemplateId: PackageService.ResolveTemplateId,
        lookupType: TypeLookup,
    )(implicit
        lc: LoggingContextOf[InstanceUUID],
        jwt: Jwt,
    ): Future[StreamPredicate[Positive]] = {

      // invariant: every set is non-empty
      def getQ(
          resolvedWithKey: Set[
            (
                com.daml.http.domain.TemplateId.RequiredPkg,
                com.daml.http.query.ValuePredicate.LfV,
            )
          ]
      ): Map[domain.TemplateId.RequiredPkg, HashSet[LfV]] =
        resolvedWithKey.foldLeft(Map.empty[domain.TemplateId.RequiredPkg, HashSet[LfV]])(
          (acc, el) =>
            acc.get(el._1) match {
              case Some(v) => acc.updated(el._1, v += el._2)
              case None => acc.updated(el._1, HashSet(el._2))
            }
        )
      def fn(
          q: Map[domain.TemplateId.RequiredPkg, HashSet[LfV]]
      ): (domain.ActiveContract[LfV], Option[domain.Offset]) => Option[Positive] = { (a, _) =>
        a.key match {
          case None => None
          case Some(k) =>
            if (q.getOrElse(a.templateId, HashSet()).contains(k)) Some(()) else None
        }
      }
      def dbQueries(
          q: Map[domain.TemplateId.RequiredPkg, HashSet[LfV]]
      )(implicit
          sjd: dbbackend.SupportedJdbcDriver
      ): Seq[(domain.TemplateId.RequiredPkg, doobie.Fragment)] =
        q.toSeq map { case (t, lfvKeys) =>
          val khd +: ktl = lfvKeys.toVector
          import dbbackend.Queries.joinFragment, com.daml.lf.crypto.Hash
          (
            t,
            joinFragment(
              OneAnd(khd, ktl) map (k =>
                keyEquality(Hash.assertHashContractKey(toLedgerApiValue(t), k))
              ),
              sql" OR ",
            ),
          )
        }
      def streamPredicate(
          q: Map[domain.TemplateId.RequiredPkg, HashSet[LfV]],
          unresolved: Set[OptionalPkg],
      ) =
        StreamPredicate(
          q.keySet,
          unresolved,
          fn(q),
          { (parties, dao) =>
            import dao.{logHandler, jdbcDriver}
            import dbbackend.ContractDao.{selectContractsMultiTemplate, MatchedQueryMarker}
            selectContractsMultiTemplate(parties, dbQueries(q), MatchedQueryMarker.Unused)
          },
        )
      request.toList
        .traverse { x: CKR[LfV] =>
          resolveTemplateId(lc)(jwt)(x.ekey.templateId)
            .map(_.toOption.flatten.map((_, x.ekey.key)).toLeft(x.ekey.templateId))
        }
        .map(
          _.toSet[Either[(RequiredPkg, LfV), OptionalPkg]].partitionMap(identity)
        )
        .map { case (resolvedWithKey, unresolved) =>
          val q = getQ(resolvedWithKey)
          streamPredicate(q, unresolved)
        }
    }

    override def renderCreatedMetadata(p: Unit) = Map.empty

    override def adjustRequest(
        prefix: Option[domain.StartingOffset],
        request: NonEmptyList[domain.ContractKeyStreamRequest[Cid, LfV]],
    ): NonEmptyList[domain.ContractKeyStreamRequest[Cid, LfV]] = request

    override def acsRequest(
        maybePrefix: Option[domain.StartingOffset],
        request: NonEmptyList[domain.ContractKeyStreamRequest[Cid, LfV]],
    ): Option[NonEmptyList[domain.ContractKeyStreamRequest[Cid, LfV]]] =
      maybePrefix.cata(_ => None, Some(request))

    override def liveStartingOffset(
        prefix: Option[domain.StartingOffset],
        request: NonEmptyList[domain.ContractKeyStreamRequest[Cid, LfV]],
    ): Option[domain.StartingOffset] = prefix

  }

  private[this] def keyEquality(k: Hash)(implicit
      sjd: dbbackend.SupportedJdbcDriver
  ): doobie.Fragment =
    sjd.queries.keyEquality(k)

  private[WebSocketService] final case class InitialEnrichedContractKeyWithStreamQuery()(implicit
      ec: ExecutionContext
  ) extends EnrichedContractKeyWithStreamQuery[Unit] {
    override def removePhantomArchives(request: NonEmptyList[CKR[LfV]]) = Some(Set.empty)
  }

  private[WebSocketService] final case class ResumingEnrichedContractKeyWithStreamQuery()(implicit
      ec: ExecutionContext
  ) extends EnrichedContractKeyWithStreamQuery[Option[Option[domain.ContractId]]] {
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
  )(implicit
      lc: LoggingContextOf[InstanceUUID],
      metrics: Metrics,
  ): Flow[Message, Message, _] =
    wsMessageHandler[A](jwt, jwtPayload)
      .via(applyConfig)
      .via(connCounter)

  private def applyConfig[A]: Flow[A, A, NotUsed] =
    Flow[A]
      .takeWithin(config.maxDuration)
      .throttle(config.throttleElem, config.throttlePer, config.maxBurst, config.mode)

  private def connCounter[A](implicit
      lc: LoggingContextOf[InstanceUUID],
      metrics: Metrics,
  ): Flow[A, A, NotUsed] =
    Flow[A]
      .watchTermination() { (_, future) =>
        discard { numConns.incrementAndGet }
        metrics.daml.HttpJsonApi.websocketRequestCounter.inc()
        logger.info(
          s"New websocket client has connected, current number of clients:${numConns.get()}"
        )
        future onComplete {
          case Success(_) =>
            discard { numConns.decrementAndGet }
            metrics.daml.HttpJsonApi.websocketRequestCounter.dec()
            logger.info(
              s"Websocket client has disconnected. Current number of clients: ${numConns.get()}"
            )
          case Failure(ex) =>
            discard { numConns.decrementAndGet }
            metrics.daml.HttpJsonApi.websocketRequestCounter.dec()
            logger.info(
              s"Websocket client interrupted on Failure: ${ex.getMessage}. remaining number of clients: ${numConns.get()}"
            )
        }
        NotUsed
      }

  private def wsMessageHandler[A: StreamQueryReader](
      jwt: Jwt,
      jwtPayload: JwtPayload,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
      _jwt: Jwt = jwt,
  ): Flow[Message, Message, NotUsed] = {
    val Q = implicitly[StreamQueryReader[A]]
    Flow[Message]
      .mapAsync(1)(parseJson)
      .via(withOptPrefix(ejv => ejv.toOption flatMap readStartingOffset))
      .mapAsync(1) { case (oeso, ejv) =>
        (for {
          offPrefix <- either[Future, Error, Option[StartingOffset]](oeso.sequence)
          jv <- either[Future, Error, JsValue](ejv)
          a <- eitherT(
            Q.parse(resumingAtOffset = offPrefix.isDefined, decoder, jv): Future[
              Error \/ Q.Query[_]
            ]
          )
        } yield (offPrefix, a: Q.Query[_])).run
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

  private[this] def fetchAndPreFilterAcs[Positive](
      predicate: StreamPredicate[Positive],
      jwt: Jwt,
      parties: OneAnd[Set, domain.Party],
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Future[Source[StepAndErrors[Positive, JsValue], NotUsed]] =
    contractsService.daoAndFetch.cata(
      { case (dao, fetch) =>
        val tx = for {
          bookmark <- fetch.fetchAndPersist(jwt, parties, predicate.resolved.toList)
          mdContracts <- predicate.dbQuery(parties, dao)
        } yield {
          val acs =
            if (mdContracts.nonEmpty) {
              Source.single(StepAndErrors(Seq.empty, ContractStreamStep.Acs(mdContracts)))
            } else {
              Source.empty
            }
          val liveMarker = liveBegin(bookmark.map(_.toDomain))
          acs ++ liveMarker
        }
        dao.transact(tx).unsafeToFuture()
      },
      Future.successful {
        contractsService
          .liveAcsAsInsertDeleteStepSource(jwt, parties, predicate.resolved.toList)
          .via(convertFilterContracts(predicate.fn))
      },
    )

  private[this] def liveBegin(
      bookmark: BeginBookmark[domain.Offset]
  ): Source[StepAndErrors[Nothing, Nothing], NotUsed] =
    Source.single(StepAndErrors(Seq.empty, ContractStreamStep.LiveBegin(bookmark)))

  private def getTransactionSourceForParty[A: StreamQuery](
      jwt: Jwt,
      parties: OneAnd[Set, domain.Party],
      offPrefix: Option[domain.StartingOffset],
      rawRequest: A,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Source[Error \/ Message, NotUsed] = {
    val Q = implicitly[StreamQuery[A]]
    implicit val _jwt: Jwt = jwt

    // If there is a prefix, replace the empty offsets in the request with it
    val request = Q.adjustRequest(offPrefix, rawRequest)

    // Take all remaining queries without offset, these will be the ones for which an ACS request is needed
    val acsRequest = Q.acsRequest(offPrefix, request)

    // Stream predicates specific fo the ACS part
    val acsPred = acsRequest.map(Q.predicate(_, resolveTemplateId, lookupType)).sequence

    def liveFrom(resolved: Set[RequiredPkg])(
        acsEnd: Option[StartingOffset]
    ): Future[Source[StepAndErrors[Q.Positive, JsValue], NotUsed]] = {
      val shiftedRequest = Q.adjustRequest(acsEnd, request)
      val liveStartingOffset = Q.liveStartingOffset(acsEnd, shiftedRequest)

      // Produce the predicate that is going to be applied to the incoming transaction stream
      // We need to apply this to the request with all the offsets shifted so that each stream
      // can filter out anything from liveStartingOffset to the query-specific offset
      Q.predicate(shiftedRequest, resolveTemplateId, lookupType).map {
        case StreamPredicate(_, _, fn, _) =>
          contractsService
            .insertDeleteStepSource(
              jwt,
              parties,
              resolved.toList,
              liveStartingOffset,
              Terminates.Never,
            )
            .via(convertFilterContracts(fn))
            .via(emitOffsetTicksAndFilterOutEmptySteps(liveStartingOffset))
      }
    }

    def processResolved(resolved: Set[RequiredPkg], unresolved: Set[OptionalPkg]) =
      acsPred
        .flatMap(
          _.map(fetchAndPreFilterAcs(_, jwt, parties))
            .cata(
              _.map { acsAndLiveMarker =>
                acsAndLiveMarker
                  .mapAsync(1) {
                    case acs @ StepAndErrors(_, Acs(_)) if acs.nonEmpty =>
                      Future.successful(Source.single(acs))
                    case StepAndErrors(_, Acs(_)) =>
                      Future.successful(Source.empty)
                    case liveBegin @ StepAndErrors(_, LiveBegin(offset)) =>
                      val acsEnd = offset.toOption.map(domain.StartingOffset(_))
                      liveFrom(resolved)(acsEnd).map(it => Source.single(liveBegin) ++ it)
                    case txn @ StepAndErrors(_, Txn(_, offset)) =>
                      val acsEnd = Some(domain.StartingOffset(offset))
                      liveFrom(resolved)(acsEnd).map(it => Source.single(txn) ++ it)
                  }
                  .flatMapConcat(it => it)
              }, {
                // This is the case where we made no ACS request because everything had an offset
                // Get the earliest available offset from where to start from
                val liveStartingOffset = Q.liveStartingOffset(offPrefix, request)
                Q.predicate(request, resolveTemplateId, lookupType).map {
                  case StreamPredicate(_, _, fn, _) =>
                    contractsService
                      .insertDeleteStepSource(
                        jwt,
                        parties,
                        resolved.toList,
                        liveStartingOffset,
                        Terminates.Never,
                      )
                      .via(convertFilterContracts(fn))
                      .via(emitOffsetTicksAndFilterOutEmptySteps(liveStartingOffset))
                }
              },
            )
            .map(
              _.via(removePhantomArchives(remove = Q.removePhantomArchives(request)))
                .map(_.mapPos(Q.renderCreatedMetadata).render)
                .prepend(reportUnresolvedTemplateIds(unresolved))
                .map(jsv => \/-(wsMessage(jsv)))
            )
        )

    Source
      .lazyFutureSource { () =>
        Q.predicate(request, resolveTemplateId, lookupType).flatMap {
          case StreamPredicate(resolved, unresolved, _, _) =>
            if (resolved.nonEmpty)
              processResolved(resolved, unresolved)
            else
              Future.successful(
                reportUnresolvedTemplateIds(unresolved)
                  .map(jsv => \/-(wsMessage(jsv)))
                  .concat(
                    Source.single(-\/(InvalidUserInput(ErrorMessages.cannotResolveAnyTemplateId)))
                  )
              )
        }
      }
      .mapMaterializedValue { _: Future[_] =>
        NotUsed
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
      fn: (domain.ActiveContract[LfV], Option[domain.Offset]) => Option[Pos]
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
              fn(ac, dstep.bookmark.flatMap(_.toOption)).map((ac, _)).toList
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
