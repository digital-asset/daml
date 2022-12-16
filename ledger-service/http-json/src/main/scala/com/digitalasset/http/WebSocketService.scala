// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.Materializer
import com.daml.fetchcontracts.util.{
  AbsoluteBookmark,
  BeginBookmark,
  ContractStreamStep,
  InsertDeleteStep,
  LedgerBegin,
}
import com.daml.fetchcontracts.util.GraphExtensions._
import com.daml.http.EndpointsCompanion._
import com.daml.http.domain.{
  ContractKeyStreamRequest,
  JwtPayload,
  SearchForeverRequest,
  StartingOffset,
}
import com.daml.http.json.{DomainJsonDecoder, JsonProtocol, SprayJson}
import com.daml.http.LedgerClientJwt.Terminates
import util.ApiValueToLfValueConverter.apiValueToLfValue
import util.toLedgerId
import ContractStreamStep.{Acs, LiveBegin, Txn}
import json.JsonProtocol.LfValueCodec.{apiValueToJsValue => lfValueToJsValue}
import query.ValuePredicate.{LfV, TypeLookup}
import com.daml.jwt.domain.Jwt
import com.daml.http.query.ValuePredicate
import doobie.ConnectionIO
import doobie.free.{connection => fconn}
import doobie.syntax.string._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.std.scalaFuture._
import scalaz.std.map._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.traverse._
import scalaz.std.list._
import scalaz.{-\/, Foldable, Liskov, NonEmptyList, Tag, \/, \/-}
import Liskov.<~<
import com.daml.fetchcontracts.domain.ResolvedQuery
import ResolvedQuery.Unsupported
import com.daml.fetchcontracts.domain.ContractTypeId.{OptionalPkg, toLedgerApiValue}
import com.daml.http.metrics.HttpJsonApiMetrics
import com.daml.http.util.FlowUtil._
import com.daml.http.util.Logging.{InstanceUUID, RequestID, extendWithRequestIdLogCtx}
import com.daml.lf.crypto.Hash
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import spray.json.{JsArray, JsObject, JsValue, JsonReader, JsonWriter, enrichAny => `sj enrichAny`}

import scala.collection.mutable.HashSet
import scala.concurrent.{ExecutionContext, Future}
import scalaz.EitherT.{either, eitherT, rightT}
import com.daml.ledger.api.{domain => LedgerApiDomain}
import com.daml.nonempty.NonEmpty

object WebSocketService {
  import util.ErrorOps._

  private val logger = ContextualizedLogger.get(getClass)

  private type CompiledQueries =
    Map[domain.ContractTypeId.Resolved, (ValuePredicate, LfV => Boolean)]

  final case class StreamPredicate[+Positive](
      resolvedQuery: domain.ResolvedQuery,
      unresolved: Set[domain.ContractTypeId.OptionalPkg],
      fn: (domain.ActiveContract.ResolvedCtTyId[LfV], Option[domain.Offset]) => Option[Positive],
      dbQuery: (domain.PartySet, dbbackend.ContractDao) => ConnectionIO[
        _ <: Vector[(domain.ActiveContract.ResolvedCtTyId[JsValue], Positive)]
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
      step: ContractStreamStep[
        domain.ArchivedContract,
        (domain.ActiveContract.ResolvedCtTyId[LfVT], Pos),
      ],
  ) {
    import JsonProtocol._

    def logHiddenErrors()(implicit lc: LoggingContextOf[InstanceUUID]): Unit =
      errors foreach { case ServerError(reason) =>
        logger.error(s"while rendering contract", reason)
      }

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
        } ++ errors.map(_ => inj("error", "error rendering contract")))
      // XXX SC ^ all useful information is now hidden;
      // can replace with an error count in later API version

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

  case class ResolvedQueryRequest[R](q: R, alg: StreamQuery[R])

  sealed abstract class StreamRequestParser[A] {
    case class QueryRequest[Q](request: Q, resolver: RequestResolver[Q])
    def parse(
        resumingAtOffset: Boolean,
        decoder: DomainJsonDecoder,
        jv: JsValue,
        jwt: Jwt,
        ledgerId: LedgerApiDomain.LedgerId,
    )(implicit
        lc: LoggingContextOf[InstanceUUID]
    ): Future[Error \/ (_ <: QueryRequest[_])]
  }

  trait RequestResolver[Q] {
    def resolve(
        req: Q,
        resolveContractTypeId: PackageService.ResolveContractTypeId,
        jwt: Jwt,
        ledgerId: LedgerApiDomain.LedgerId,
    )(implicit
        lc: LoggingContextOf[InstanceUUID]
    ): Future[Error \/ ResolvedQueryRequest[_]]
  }

  sealed trait StreamQuery[R] {

    /** Extra data on success of a predicate. */
    type Positive

    def removePhantomArchives(resolvedRequest: R): Option[Set[domain.ContractId]]

    private[WebSocketService] def predicate(
        resolvedRequest: R,
        resolveContractTypeId: PackageService.ResolveContractTypeId,
        lookupType: ValuePredicate.TypeLookup,
        jwt: Jwt,
        ledgerId: LedgerApiDomain.LedgerId,
    )(implicit
        lc: LoggingContextOf[InstanceUUID]
    ): Future[StreamPredicate[Positive]]

    def renderCreatedMetadata(p: Positive): Map[String, JsValue]

    def acsRequest(
        maybePrefix: Option[domain.StartingOffset],
        resolvedRequest: R,
    ): Option[R]

    /** Perform any necessary adjustment to the request based on the prefix
      */
    def adjustRequest(
        prefix: Option[domain.StartingOffset],
        resolvedRequest: R,
    ): R

    /** Specify the offset from which the live part of the query should start
      */
    def liveStartingOffset(
        prefix: Option[domain.StartingOffset],
        resolvedRequest: R,
    ): Option[domain.StartingOffset]
  }

  final case class ResolvedSearchForeverRequest(
      resolvedQuery: ResolvedQuery,
      queriesWithPos: NonEmpty[List[(ResolvedSearchForeverQuery, Int)]],
      unresolved: Set[domain.ContractTypeId.OptionalPkg],
  )

  final case class ResolvedSearchForeverQuery(
      resolvedQuery: ResolvedQuery,
      query: Map[String, JsValue],
      offset: Option[domain.Offset],
  )

  implicit def SearchForeverRequestWithStreamQuery(implicit
      ec: ExecutionContext
  ): StreamRequestParser[domain.SearchForeverRequest] =
    new StreamRequestParser[domain.SearchForeverRequest]
      with StreamQuery[ResolvedSearchForeverRequest]
      with RequestResolver[domain.SearchForeverRequest] {
      type Positive = NonEmptyList[Int]

      override def parse(
          resumingAtOffset: Boolean,
          decoder: DomainJsonDecoder,
          jv: JsValue,
          jwt: Jwt,
          ledgerId: LedgerApiDomain.LedgerId,
      )(implicit
          lc: LoggingContextOf[InstanceUUID]
      ) = {
        import JsonProtocol._
        Future.successful(
          SprayJson
            .decode[SearchForeverRequest](jv)
            .liftErr[Error](InvalidUserInput)
            .map(QueryRequest(_, this))
        )
      }

      override def resolve(
          req: SearchForeverRequest,
          resolveContractTypeId: PackageService.ResolveContractTypeId,
          jwt: Jwt,
          ledgerId: LedgerApiDomain.LedgerId,
      )(implicit
          lc: LoggingContextOf[InstanceUUID]
      ): Future[Error \/ ResolvedQueryRequest[_]] = {
        import scalaz.syntax.foldable._

        def resolveIds(
            sfq: domain.SearchForeverQuery
        ): Future[(Set[domain.ContractTypeId.Resolved], Set[domain.ContractTypeId.OptionalPkg])] =
          sfq.templateIds.toList.toNEF
            .traverse(x =>
              resolveContractTypeId(jwt, ledgerId)(x).map(_.toOption.flatten.toLeft(x))
            )
            .map(
              _.toSet.partitionMap(
                identity[
                  Either[domain.ContractTypeId.Resolved, domain.ContractTypeId.OptionalPkg]
                ]
              )
            )

        def query(
            sfq: domain.SearchForeverQuery,
            pos: Int,
        ): Future[
          Unsupported \/ (
              ResolvedSearchForeverQuery,
              Int,
              Set[domain.ContractTypeId.OptionalPkg],
          )
        ] = {
          (for {
            partitionedResolved <- resolveIds(sfq)
            (resolved, unresolved) = partitionedResolved
            res = domain.ResolvedQuery(resolved).map { rq =>
              (
                ResolvedSearchForeverQuery(
                  rq,
                  sfq.query,
                  sfq.offset,
                ),
                pos,
                unresolved,
              )
            }
          } yield res)
        }

        Future
          .sequence(
            req.queriesWithPos
              .map((query _).tupled)
              .toList
          )
          .map { l =>
            val (
              err: List[Unsupported],
              ok: List[
                (
                    ResolvedSearchForeverQuery,
                    Int,
                    Set[domain.ContractTypeId.OptionalPkg],
                )
              ],
            ) =
              l.partitionMap(_.toEither)
            if (err.nonEmpty) -\/(InvalidUserInput(err.map(_.errorMsg).mkString))
            else if (ok.isEmpty) -\/(InvalidUserInput(ResolvedQuery.CannotBeEmpty.errorMsg))
            else {
              val queriesWithPos = ok.map { case (q, p, _) => (q, p) }
              val unresolved = ok.flatMap { case (_, _, unresolved) => unresolved }.toSet
              val resolvedQuery =
                ResolvedQuery(ok.flatMap { case (q, _, _) => q.resolvedQuery.resolved }.toSet)
                  .leftMap(unsupported => InvalidUserInput(unsupported.errorMsg))
              resolvedQuery.flatMap { rq =>
                queriesWithPos match {
                  case NonEmpty(list) =>
                    \/-(
                      ResolvedQueryRequest(
                        ResolvedSearchForeverRequest(rq, list, unresolved),
                        this,
                      )
                    )
                  case _ => -\/(InvalidUserInput(ResolvedQuery.CannotBeEmpty.errorMsg))
                }
              }
            }
          }
      }

      override def removePhantomArchives(request: ResolvedSearchForeverRequest) = None

      override private[WebSocketService] def predicate(
          request: ResolvedSearchForeverRequest,
          resolveContractTypeId: PackageService.ResolveContractTypeId,
          lookupType: ValuePredicate.TypeLookup,
          jwt: Jwt,
          ledgerId: LedgerApiDomain.LedgerId,
      )(implicit
          lc: LoggingContextOf[InstanceUUID]
      ): Future[StreamPredicate[Positive]] = {

        import scalaz.syntax.foldable._
        import util.Collections._

        val indexedOffsets: Vector[Option[domain.Offset]] =
          request.queriesWithPos.map { case (q, _) => q.offset }.toVector

        def matchesOffset(queryIndex: Int, maybeEventOffset: Option[domain.Offset]): Boolean = {
          import domain.Offset.`Offset ordering`
          import scalaz.syntax.order._
          val matches =
            for {
              queryOffset <- indexedOffsets(queryIndex)
              eventOffset <- maybeEventOffset
            } yield eventOffset > queryOffset
          matches.getOrElse(true)
        }

        def fn(
            q: Map[domain.ContractTypeId.Resolved, NonEmptyList[
              ((ValuePredicate, LfV => Boolean), (Int, Int))
            ]]
        )(
            a: domain.ActiveContract.ResolvedCtTyId[LfV],
            o: Option[domain.Offset],
        ): Option[Positive] = {
          q.get(a.templateId).flatMap { preds =>
            preds.collect(Function unlift { case ((_, p), (ix, pos)) =>
              val matchesPredicate = p(a.payload)
              (matchesPredicate && matchesOffset(ix, o)).option(pos)
            })
          }
        }

        def dbQueriesPlan[CtId <: domain.ContractTypeId.RequiredPkg](
            q: Map[CtId, NonEmptyList[((ValuePredicate, LfV => Boolean), (Int, Int))]]
        )(implicit
            sjd: dbbackend.SupportedJdbcDriver.TC
        ): (Seq[(CtId, doobie.Fragment)], Map[Int, Int]) = {
          val annotated = q.toSeq.flatMap { case (tpid, nel) =>
            nel.toVector.map { case ((vp, _), (_, pos)) => (tpid, vp.toSqlWhereClause, pos) }
          }
          val posMap = annotated.iterator.zipWithIndex.map { case ((_, _, pos), ix) =>
            (ix, pos)
          }.toMap
          (annotated map { case (tpid, sql, _) => (tpid, sql) }, posMap)
        }

        def query(
            rsfq: ResolvedSearchForeverQuery,
            pos: Int,
            ix: Int,
        ): Map[domain.ContractTypeId.Resolved, NonEmptyList[
          ((ValuePredicate, ValuePredicate.LfV => Boolean), (Int, Int))
        ]] = {
          val compiledQueries = prepareFilters(rsfq.resolvedQuery.resolved, rsfq.query, lookupType)
          compiledQueries.transform((_, p) => NonEmptyList((p, (ix, pos))))
        }

        val q =
          request.queriesWithPos.zipWithIndex // index is used to ensure matchesOffset works properly
            .map { case ((q, pos), ix) => (q, pos, ix) }
            .toNEF
            .foldMap((query _).tupled)

        Future.successful(
          StreamPredicate(
            request.resolvedQuery,
            request.unresolved,
            fn(q),
            { (parties, dao) =>
              import dao.{logHandler, jdbcDriver}
              import dbbackend.ContractDao.{selectContractsMultiTemplate, MatchedQueryMarker}
              val (dbQueries, posMap) = dbQueriesPlan(q)
              selectContractsMultiTemplate(parties, dbQueries, MatchedQueryMarker.ByNelInt)
                .map(_ map (_ rightMap (_ map posMap)))
            },
          )
        )
      }

      private def prepareFilters(
          resolved: Set[domain.ContractTypeId.Resolved],
          queryExpr: Map[String, JsValue],
          lookupType: ValuePredicate.TypeLookup,
      ): CompiledQueries =
        resolved.iterator.map { tid =>
          val vp = ValuePredicate.fromTemplateJsObject(queryExpr, tid, lookupType)
          (tid, (vp, vp.toFunPredicate))
        }.toMap

      override def renderCreatedMetadata(p: Positive) =
        Map {
          import JsonProtocol._
          "matchedQueries" -> p.toJson
        }

      override def acsRequest(
          maybePrefix: Option[domain.StartingOffset],
          request: ResolvedSearchForeverRequest,
      ): Option[ResolvedSearchForeverRequest] = {
        val withoutOffset =
          NonEmpty.from(request.queriesWithPos.filter { case (q, _) => q.offset.isEmpty })

        withoutOffset.map(
          ResolvedSearchForeverRequest(request.resolvedQuery, _, request.unresolved)
        )
      }

      override def adjustRequest(
          prefix: Option[domain.StartingOffset],
          request: ResolvedSearchForeverRequest,
      ): ResolvedSearchForeverRequest =
        prefix.fold(request)(prefix =>
          request.copy(
            queriesWithPos = request.queriesWithPos.map {
              _ leftMap (q => q.copy(offset = q.offset.orElse(Some(prefix.offset))))
            }
          )
        )

      import scalaz.syntax.foldable1._
      import domain.Offset.`Offset ordering`
      import scalaz.std.option.optionOrder

      // This is called after `adjustRequest` already filled in the blank offsets
      override def liveStartingOffset(
          prefix: Option[domain.StartingOffset],
          request: ResolvedSearchForeverRequest,
      ): Option[domain.StartingOffset] =
        request.queriesWithPos
          .map { case (q, _) => q.offset }
          .toNEF
          .minimumBy1(identity)
          .map(domain.StartingOffset(_))

    }

  case class ResolvedContractKeyStreamRequest[C, V](
      resolvedQuery: ResolvedQuery,
      list: NonEmptyList[domain.ContractKeyStreamRequest[C, V]],
      q: Map[domain.ContractTypeId.Resolved, HashSet[V]],
      unresolved: Set[domain.ContractTypeId.OptionalPkg],
  )

  implicit def EnrichedContractKeyWithStreamQuery(implicit
      ec: ExecutionContext
  ): StreamRequestParser[domain.ContractKeyStreamRequest[_, _]] =
    new StreamRequestParser[domain.ContractKeyStreamRequest[_, _]] {
      import JsonProtocol._

      override def parse(
          resumingAtOffset: Boolean,
          decoder: DomainJsonDecoder,
          jv: JsValue,
          jwt: Jwt,
          ledgerId: LedgerApiDomain.LedgerId,
      )(implicit
          lc: LoggingContextOf[InstanceUUID]
      ) = {
        type NelCKRH[Hint, V] = NonEmptyList[domain.ContractKeyStreamRequest[Hint, V]]
        def go[Hint](
            resolver: RequestResolver[NelCKRH[Hint, LfV]]
        )(implicit ev: JsonReader[NelCKRH[Hint, JsValue]]) =
          for {
            as <- either[Future, Error, NelCKRH[Hint, JsValue]](
              SprayJson
                .decode[NelCKRH[Hint, JsValue]](jv)
                .liftErr[Error](InvalidUserInput)
            )
            bs <- rightT {
              as.map(a => decodeWithFallback(decoder, a, jwt, ledgerId)).sequence
            }
          } yield QueryRequest(bs, resolver)
        if (resumingAtOffset) go(ResumingEnrichedContractKeyWithStreamQuery())
        else go(InitialEnrichedContractKeyWithStreamQuery())
      }.run

      private def decodeWithFallback[Hint](
          decoder: DomainJsonDecoder,
          a: domain.ContractKeyStreamRequest[Hint, JsValue],
          jwt: Jwt,
          ledgerId: LedgerApiDomain.LedgerId,
      )(implicit
          lc: LoggingContextOf[InstanceUUID]
      ): Future[domain.ContractKeyStreamRequest[Hint, domain.LfValue]] =
        decoder
          .decodeUnderlyingValuesToLf(a, jwt, ledgerId)
          .run
          .map(
            _.valueOr(_ => a.map(_ => com.daml.lf.value.Value.ValueUnit))
          ) // unit will not match any key
    }

  private[this] sealed abstract class EnrichedContractKeyWithStreamQuery[Cid](implicit
      ec: ExecutionContext
  ) extends StreamQuery[ResolvedContractKeyStreamRequest[Cid, LfV]]
      with RequestResolver[NonEmptyList[domain.ContractKeyStreamRequest[Cid, LfV]]] {
    type Positive = Unit

    protected type CKR[+V] = domain.ContractKeyStreamRequest[Cid, V]

    override def resolve(
        request: NonEmptyList[ContractKeyStreamRequest[Cid, LfV]],
        resolveContractTypeId: PackageService.ResolveContractTypeId,
        jwt: Jwt,
        ledgerId: LedgerApiDomain.LedgerId,
    )(implicit
        lc: LoggingContextOf[InstanceUUID]
    ): Future[Error \/ ResolvedQueryRequest[_]] = {
      def getQ[K, V](resolvedWithKey: Set[(K, V)]): Map[K, HashSet[V]] =
        resolvedWithKey.to(HashSet).groupMap(_._1)(_._2)

      request.toList
        .traverse { x: CKR[LfV] =>
          resolveContractTypeId(jwt, ledgerId)(x.ekey.templateId)
            .map(_.toOption.flatten.map((_, x.ekey.key)).toLeft(x.ekey.templateId))
        }
        .map { resolveTries =>
          val (resolvedWithKey, unresolved) = resolveTries
            .toSet[Either[(domain.ContractTypeId.Resolved, LfV), OptionalPkg]]
            .partitionMap(identity)
          val q = getQ(resolvedWithKey)
          domain
            .ResolvedQuery(q.keySet)
            .leftMap(unsupported => InvalidUserInput(unsupported.errorMsg))
            .map { rq =>
              ResolvedQueryRequest(
                ResolvedContractKeyStreamRequest(rq, request, q, unresolved),
                this,
              )
            }
        }
    }

    override private[WebSocketService] def predicate(
        resolvedRequest: ResolvedContractKeyStreamRequest[Cid, LfV],
        resolveContractTypeId: PackageService.ResolveContractTypeId,
        lookupType: TypeLookup,
        jwt: Jwt,
        ledgerId: LedgerApiDomain.LedgerId,
    )(implicit
        lc: LoggingContextOf[InstanceUUID]
    ): Future[StreamPredicate[Positive]] = {
      def fn(
          q: Map[domain.ContractTypeId.Resolved, HashSet[LfV]]
      ): (domain.ActiveContract.ResolvedCtTyId[LfV], Option[domain.Offset]) => Option[Positive] = {
        (a, _) =>
          a.key match {
            case None => None
            case Some(k) =>
              if (q.getOrElse(a.templateId, HashSet()).contains(k)) Some(()) else None
          }
      }
      def dbQueries[CtId <: domain.ContractTypeId.RequiredPkg](
          q: Map[CtId, HashSet[LfV]]
      )(implicit
          sjd: dbbackend.SupportedJdbcDriver.TC
      ): Seq[(CtId, doobie.Fragment)] =
        q.toSeq map { case (t, lfvKeys) =>
          val NonEmpty(keys) = lfvKeys.toVector
          import dbbackend.Queries.joinFragment, com.daml.lf.crypto.Hash
          (
            t,
            joinFragment(
              keys map (k => keyEquality(Hash.assertHashContractKey(toLedgerApiValue(t), k))),
              sql" OR ",
            ),
          )
        }

      Future.successful(
        StreamPredicate[Positive](
          resolvedRequest.resolvedQuery,
          resolvedRequest.unresolved,
          fn(resolvedRequest.q),
          { (parties, dao) =>
            import dao.{logHandler, jdbcDriver}
            import dbbackend.ContractDao.{selectContractsMultiTemplate, MatchedQueryMarker}
            selectContractsMultiTemplate(
              parties,
              dbQueries(resolvedRequest.q),
              MatchedQueryMarker.Unused,
            )
          },
        )
      )
    }

    override def renderCreatedMetadata(p: Unit) = Map.empty

    override def adjustRequest(
        prefix: Option[domain.StartingOffset],
        request: ResolvedContractKeyStreamRequest[Cid, LfV],
    ): ResolvedContractKeyStreamRequest[Cid, LfV] = request

    override def acsRequest(
        maybePrefix: Option[domain.StartingOffset],
        request: ResolvedContractKeyStreamRequest[Cid, LfV],
    ): Option[ResolvedContractKeyStreamRequest[Cid, LfV]] =
      maybePrefix.cata(_ => None, Some(request))

    override def liveStartingOffset(
        prefix: Option[domain.StartingOffset],
        request: ResolvedContractKeyStreamRequest[Cid, LfV],
    ): Option[domain.StartingOffset] = prefix

  }

  private[this] def keyEquality(k: Hash)(implicit
      sjd: dbbackend.SupportedJdbcDriver.TC
  ): doobie.Fragment =
    sjd.q.queries.keyEquality(k)

  private[WebSocketService] final class InitialEnrichedContractKeyWithStreamQuery private ()(
      implicit ec: ExecutionContext
  ) extends EnrichedContractKeyWithStreamQuery[Unit] {
    override def removePhantomArchives(request: ResolvedContractKeyStreamRequest[Unit, LfV]) =
      Some(Set.empty)
  }

  object InitialEnrichedContractKeyWithStreamQuery {
    def apply()(implicit ec: ExecutionContext) = new InitialEnrichedContractKeyWithStreamQuery()
  }

  private[WebSocketService] final class ResumingEnrichedContractKeyWithStreamQuery private ()(
      implicit ec: ExecutionContext
  ) extends EnrichedContractKeyWithStreamQuery[Option[Option[domain.ContractId]]] {
    override def removePhantomArchives(
        request: ResolvedContractKeyStreamRequest[Option[Option[domain.ContractId]], LfV]
    ) = {
      val NelO = Foldable[NonEmptyList].compose[Option]
      request.list traverse (_.contractIdAtOffset) map NelO.toSet
    }
  }

  object ResumingEnrichedContractKeyWithStreamQuery {
    def apply()(implicit ec: ExecutionContext) = new ResumingEnrichedContractKeyWithStreamQuery()
  }

  private abstract sealed class TickTriggerOrStep[+A] extends Product with Serializable
  private final case object TickTrigger extends TickTriggerOrStep[Nothing]
  private final case class Step[A](payload: StepAndErrors[A, JsValue]) extends TickTriggerOrStep[A]
}

class WebSocketService(
    contractsService: ContractsService,
    resolveContractTypeId: PackageService.ResolveContractTypeId,
    decoder: DomainJsonDecoder,
    lookupType: ValuePredicate.TypeLookup,
    wsConfig: Option[WebsocketConfig],
)(implicit mat: Materializer, ec: ExecutionContext) {

  import WebSocketService._
  import com.daml.scalautil.Statement.discard
  import util.ErrorOps._
  import com.daml.http.json.JsonProtocol._

  private val config = wsConfig.getOrElse(WebsocketConfig())

  private val numConns = new java.util.concurrent.atomic.AtomicInteger(0)

  private[http] def transactionMessageHandler[A: StreamRequestParser](
      jwt: Jwt,
      jwtPayload: JwtPayload,
  )(implicit
      lc: LoggingContextOf[InstanceUUID],
      metrics: HttpJsonApiMetrics,
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
      metrics: HttpJsonApiMetrics,
  ): Flow[A, A, NotUsed] =
    Flow[A]
      .watchTermination() { (_, future) =>
        val afterInc = numConns.incrementAndGet()
        metrics.websocketRequestCounter.inc()
        logger.info(
          s"New websocket client has connected, current number of clients:$afterInc"
        )
        future onComplete { td =>
          def msg = td.fold(
            ex => s"interrupted on Failure: ${ex.getMessage}. remaining",
            _ => "has disconnected. Current",
          )
          val afterDec = numConns.decrementAndGet()
          metrics.websocketRequestCounter.dec()
          logger.info(s"Websocket client $msg number of clients: $afterDec")
        }
        NotUsed
      }

  private def wsMessageHandler[A: StreamRequestParser](
      jwt: Jwt,
      jwtPayload: JwtPayload,
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): Flow[Message, Message, NotUsed] = {
    val Q = implicitly[StreamRequestParser[A]]
    Flow[Message]
      .mapAsync(1)(parseJson)
      .via(withOptPrefix(ejv => ejv.toOption flatMap readStartingOffset))
      .mapAsync(1) { case (oeso, ejv) =>
        (for {
          offPrefix <- either[Future, Error, Option[StartingOffset]](oeso.sequence)
          jv <- either[Future, Error, JsValue](ejv)
          a <- eitherT(
            Q.parse(
              resumingAtOffset = offPrefix.isDefined,
              decoder,
              jv,
              jwt,
              toLedgerId(jwtPayload.ledgerId),
            ): Future[
              Error \/ Q.QueryRequest[_]
            ]
          )
        } yield (offPrefix, a: Q.QueryRequest[_])).run
      }
      .mapAsync(1) {
        _.map { case (offPrefix, qq: Q.QueryRequest[q]) =>
          qq.resolver
            .resolve(
              qq.request,
              resolveContractTypeId,
              jwt,
              toLedgerId(jwtPayload.ledgerId),
            )
            .map(_.map(resolved => (offPrefix, resolved)))
        }
          .fold(e => Future.successful(-\/(e)), identity)
      }
      .via(
        allowOnlyFirstInput(
          InvalidUserInput("Multiple requests over the same WebSocket connection are not allowed.")
        )
      )
      .flatMapMergeCancellable(
        2, // 2 streams max, the 2nd is to be able to send an error back
        _.map { case (offPrefix, rq: ResolvedQueryRequest[q]) =>
          implicit val SQ: StreamQuery[q] = rq.alg
          getTransactionSourceForParty[q](
            jwt,
            toLedgerId(jwtPayload.ledgerId),
            jwtPayload.parties,
            offPrefix,
            rq.q: q,
          ).logTermination("GTSFP-outer")
        }.valueOr(e => Source.single(-\/(e))): Source[Error \/ Message, NotUsed],
      )
      .takeWhile(_.isRight, inclusive = true) // stop after emitting 1st error
      .map(
        _.fold(e => extendWithRequestIdLogCtx(implicit lc1 => wsErrorMessage(e)), identity): Message
      )
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
      ledgerId: LedgerApiDomain.LedgerId,
      parties: domain.PartySet,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Future[Source[StepAndErrors[Positive, JsValue], NotUsed]] = {
    contractsService.daoAndFetch.cata(
      { case (dao, fetch) =>
        val tx: ConnectionIO[Source[StepAndErrors[Positive, JsValue], NotUsed]] =
          fetch.fetchAndPersistBracket(
            jwt,
            ledgerId,
            parties,
            predicate.resolvedQuery.resolved.toList,
          ) {
            case LedgerBegin =>
              fconn.pure(liveBegin(LedgerBegin))
            case bookmark @ AbsoluteBookmark(_) =>
              for {
                mdContracts <- predicate.dbQuery(parties, dao)
              } yield {
                val acs =
                  if (mdContracts.nonEmpty)
                    Source.single(StepAndErrors(Seq.empty, ContractStreamStep.Acs(mdContracts)))
                  else
                    Source.empty
                val liveMarker = liveBegin(bookmark.map(_.toDomain))
                acs ++ liveMarker
              }
          }
        dao.transact(tx).unsafeToFuture()
      },
      Future.successful {
        contractsService
          .liveAcsAsInsertDeleteStepSource(
            jwt,
            ledgerId,
            parties,
            predicate.resolvedQuery.resolved.toList,
          )
          .via(
            convertFilterContracts(
              predicate.resolvedQuery,
              predicate.fn,
            )
          )
      },
    )
  }

  private[this] def liveBegin(
      bookmark: BeginBookmark[domain.Offset]
  ): Source[StepAndErrors[Nothing, Nothing], NotUsed] =
    Source.single(StepAndErrors(Seq.empty, ContractStreamStep.LiveBegin(bookmark)))

  // simple alias to avoid passing in the class parameters
  private[this] def queryPredicate[A](
      request: A,
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(implicit
      lc: LoggingContextOf[InstanceUUID],
      Q: StreamQuery[A],
  ): Future[StreamPredicate[Q.Positive]] =
    Q.predicate(request, resolveContractTypeId, lookupType, jwt, ledgerId)

  private def getTransactionSourceForParty[A](
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: domain.PartySet,
      offPrefix: Option[domain.StartingOffset],
      rawRequest: A,
  )(implicit
      lc: LoggingContextOf[InstanceUUID],
      Q: StreamQuery[A],
  ): Source[Error \/ Message, NotUsed] = {
    // If there is a prefix, replace the empty offsets in the request with it
    val request = Q.adjustRequest(offPrefix, rawRequest)

    // Take all remaining queries without offset, these will be the ones for which an ACS request is needed
    val acsRequest = Q.acsRequest(offPrefix, request)

    // Stream predicates specific fo the ACS part
    val acsPred =
      acsRequest
        .map(queryPredicate(_, jwt, ledgerId))
        .sequence

    def liveFrom(resolvedQuery: ResolvedQuery)(
        acsEnd: Option[StartingOffset]
    ): Future[Source[StepAndErrors[Q.Positive, JsValue], NotUsed]] = {
      val shiftedRequest = Q.adjustRequest(acsEnd, request)
      val liveStartingOffset = Q.liveStartingOffset(acsEnd, shiftedRequest)

      // Produce the predicate that is going to be applied to the incoming transaction stream
      // We need to apply this to the request with all the offsets shifted so that each stream
      // can filter out anything from liveStartingOffset to the query-specific offset
      queryPredicate(shiftedRequest, jwt, ledgerId).map { case StreamPredicate(_, _, fn, _) =>
        contractsService
          .insertDeleteStepSource(
            jwt,
            ledgerId,
            parties,
            resolvedQuery.resolved.toList,
            liveStartingOffset,
            Terminates.Never,
          )
          .logTermination("IDSS-outer-1")
          .via(
            convertFilterContracts(
              resolvedQuery,
              fn,
            )
          )
          .via(emitOffsetTicksAndFilterOutEmptySteps(liveStartingOffset))
      }
    }

    def processResolved(
        resolvedQuery: ResolvedQuery,
        unresolved: Set[OptionalPkg],
        fn: (domain.ActiveContract.ResolvedCtTyId[LfV], Option[domain.Offset]) => Option[Q.Positive],
    ) =
      acsPred
        .flatMap(
          _.flatMap { vp: StreamPredicate[Q.Positive] =>
            Some(fetchAndPreFilterAcs(vp, jwt, ledgerId, parties))
          }.cata(
            _.map { acsAndLiveMarker =>
              acsAndLiveMarker
                .mapAsync(1) {
                  case acs @ StepAndErrors(_, Acs(_)) if acs.nonEmpty =>
                    Future.successful(Source.single(acs))
                  case StepAndErrors(_, Acs(_)) =>
                    Future.successful(Source.empty)
                  case liveBegin @ StepAndErrors(_, LiveBegin(offset)) =>
                    val acsEnd = offset.toOption.map(domain.StartingOffset(_))
                    liveFrom(resolvedQuery)(acsEnd).map(it => Source.single(liveBegin) ++ it)
                  case txn @ StepAndErrors(_, Txn(_, offset)) =>
                    val acsEnd = Some(domain.StartingOffset(offset))
                    liveFrom(resolvedQuery)(acsEnd).map(it => Source.single(txn) ++ it)
                }
                .flatMapConcat(it => it)
            }, {
              // This is the case where we made no ACS request because everything had an offset
              // Get the earliest available offset from where to start from
              val liveStartingOffset = Q.liveStartingOffset(offPrefix, request)
              Future.successful(
                contractsService
                  .insertDeleteStepSource(
                    jwt,
                    ledgerId,
                    parties,
                    resolvedQuery.resolved.toList,
                    liveStartingOffset,
                    Terminates.Never,
                  )
                  .logTermination("IDSS-outer-2")
                  .via(
                    convertFilterContracts(
                      resolvedQuery,
                      fn,
                    )
                  )
                  .via(emitOffsetTicksAndFilterOutEmptySteps(liveStartingOffset))
              )
            },
          ).map(
            _.via(removePhantomArchives(remove = Q.removePhantomArchives(request)))
              .map { sae =>
                sae.logHiddenErrors()
                sae.mapPos(Q.renderCreatedMetadata).render
              }
              .prepend(reportUnresolvedTemplateIds(unresolved))
              .map(jsv => \/-(wsMessage(jsv)))
          )
        )

    Source
      .lazyFutureSource { () =>
        queryPredicate(request, jwt, ledgerId).flatMap { pred =>
          processResolved(pred.resolvedQuery, pred.unresolved, pred.fn)
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
      offset.map(o => AbsoluteBookmark(o.offset)): Option[BeginBookmark[domain.Offset]],
      TickTrigger: TickTriggerOrStep[Pos],
    )

    Flow[StepAndErrors[Pos, JsValue]]
      .map(a => Step(a))
      .keepAlive(config.heartbeatPeriod, () => TickTrigger)
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

  private[http] def wsErrorMessage(error: Error)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): TextMessage.Strict =
    wsMessage(SprayJson.encodeUnsafe(errorResponse(error)))

  private[http] def wsMessage(jsVal: JsValue): TextMessage.Strict =
    TextMessage(jsVal.compactPrint)

  private def convertFilterContracts[Pos](
      resolvedQuery: domain.ResolvedQuery,
      fn: (domain.ActiveContract.ResolvedCtTyId[LfV], Option[domain.Offset]) => Option[Pos],
  ): Flow[ContractStreamStep.LAV1, StepAndErrors[Pos, JsValue], NotUsed] =
    Flow
      .fromFunction { step: ContractStreamStep.LAV1 =>
        val (aerrors, errors, dstep) = step.partitionBimap(
          ae =>
            domain.ArchivedContract
              .fromLedgerApi(resolvedQuery, ae)
              .liftErr(ServerError.fromMsg),
          ce =>
            domain.ActiveContract
              .fromLedgerApi(resolvedQuery, ce)
              .liftErr(ServerError.fromMsg)
              .flatMap(_.traverse(apiValueToLfValue).liftErr(ServerError.fromMsg)),
        )(Seq)
        StepAndErrors(
          errors ++ aerrors,
          dstep mapInserts { inserts: Vector[domain.ActiveContract.ResolvedCtTyId[LfV]] =>
            inserts.flatMap { ac =>
              fn(ac, dstep.bookmark.flatMap(_.toOption)).map((ac, _)).toList
            }
          },
        )
      }
      .via(conflation)
      .map(_ mapLfv lfValueToJsValue)

  private def reportUnresolvedTemplateIds(
      unresolved: Set[domain.ContractTypeId.OptionalPkg]
  ): Source[JsValue, NotUsed] =
    if (unresolved.isEmpty) Source.empty
    else {
      Source.single(
        domain.AsyncWarningsWrapper(domain.UnknownTemplateIds(unresolved.toList)).toJson
      )
    }
}
