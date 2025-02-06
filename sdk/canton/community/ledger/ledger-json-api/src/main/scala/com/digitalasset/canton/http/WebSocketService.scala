// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.jwt.Jwt
import com.daml.logging.LoggingContextOf
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyColl.foldable1
import com.digitalasset.canton.fetchcontracts.util.GraphExtensions.*
import com.digitalasset.canton.fetchcontracts.util.{
  AbsoluteBookmark,
  BeginBookmark,
  ContractStreamStep,
  InsertDeleteStep,
}
import com.digitalasset.canton.http.ContractTypeId.RequiredPkg
import com.digitalasset.canton.http.EndpointsCompanion.*
import com.digitalasset.canton.http.LedgerClientJwt.Terminates
import com.digitalasset.canton.http.ResolvedQuery.Unsupported
import com.digitalasset.canton.http.json.{ApiJsonDecoder, JsonProtocol, SprayJson}
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.http.util.FlowUtil.allowOnlyFirstInput
import com.digitalasset.canton.http.util.Logging.{
  InstanceUUID,
  RequestID,
  extendWithRequestIdLogCtx,
}
import com.digitalasset.canton.http.{
  ContractKeyStreamRequest,
  JwtPayload,
  ResolvedQuery,
  SearchForeverRequest,
  StartingOffset,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import scalaz.EitherT.{either, eitherT, rightT}
import scalaz.std.list.*
import scalaz.std.map.*
import scalaz.std.option.*
import scalaz.std.scalaFuture.*
import scalaz.std.tuple.*
import scalaz.syntax.bifunctor.*
import scalaz.syntax.std.boolean.*
import scalaz.syntax.std.option.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, Foldable, Liskov, NonEmptyList, Tag, \/, \/-}
import spray.json.{JsArray, JsObject, JsValue, JsonReader, JsonWriter, enrichAny as `sj enrichAny`}

import scala.concurrent.{ExecutionContext, Future}

import util.ApiValueToLfValueConverter.apiValueToLfValue
import ContractStreamStep.{Acs, LiveBegin, Txn}
import json.JsonProtocol.LfValueCodec.apiValueToJsValue as lfValueToJsValue
import query.ValuePredicate.LfV
import Liskov.<~<

object WebSocketService extends NoTracing {
  import com.digitalasset.canton.http.util.ErrorOps.*

  final case class StreamPredicate[+Positive](
      resolvedQuery: ResolvedQuery,
      unresolved: Set[ContractTypeId.RequiredPkg],
      fn: (ActiveContract.ResolvedCtTyId[LfV], Option[Offset]) => Option[Positive],
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
        ArchivedContract,
        (ActiveContract.ResolvedCtTyId[LfVT], Pos),
      ],
      loggerFactory: NamedLoggerFactory,
  ) extends NamedLogging {
    import JsonProtocol.*

    def logHiddenErrors()(implicit lc: LoggingContextOf[InstanceUUID]): Unit =
      errors foreach { case ServerError(reason) =>
        logger.error(s"while rendering contract, ${lc.makeString}", reason)
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
          acj.copy(fields = acj.fields ++ pos)
        } ++ errors.map(_ => inj("error", "error rendering contract")))
      // TODO(i13377) ^ all useful information is now hidden;
      // can replace with an error count in later API version

      val offsetAfter = step.bookmark.map(_.toJson)

      renderEvents(events, offsetAfter)
    }

    def append[P >: Pos, A >: LfVT](o: StepAndErrors[P, A]): StepAndErrors[P, A] =
      StepAndErrors(errors ++ o.errors, step append o.step, loggerFactory)

    def mapLfv[A](f: LfVT => A): StepAndErrors[Pos, A] =
      copy(step = step mapPreservingIds (_ leftMap (_ map f)))

    def mapPos[P](f: Pos => P): StepAndErrors[P, LfVT] =
      copy(step = step mapPreservingIds (_ rightMap f))

    def nonEmpty: Boolean = errors.nonEmpty || step.nonEmpty
  }

  private def renderEvents(events: Vector[JsObject], offset: Option[JsValue]): JsObject =
    JsObject(Map("events" -> JsArray(events)) ++ offset.map("offset" -> _).toList)

  private def readStartingOffset(jv: JsValue): Option[Error \/ StartingOffset] =
    jv match {
      case JsObject(fields) =>
        fields get "offset" map { offJv =>
          import JsonProtocol.*
          if (fields.sizeIs > 1)
            -\/(InvalidUserInput("offset must be specified as a leading, separate object message"))
          else
            SprayJson
              .decode[Offset](offJv)
              .liftErr[Error](InvalidUserInput.apply)
              .map(offset => StartingOffset(offset))
        }
      case _ => None
    }

  private def conflation[P, A]: Flow[StepAndErrors[P, A], StepAndErrors[P, A], NotUsed] = {
    val maxCost = 200L
    Flow[StepAndErrors[P, A]]
      .batchWeighted(
        max = maxCost,
        costFn = {
          case StepAndErrors(errors, ContractStreamStep.LiveBegin(_), _) =>
            1L + errors.length
          case StepAndErrors(errors, step, _) =>
            val InsertDeleteStep(inserts, deletes) = step.toInsertDelete
            errors.length.toLong + (inserts.length * 2) + deletes.size
        },
        identity,
      )(_ append _)
  }

  final case class ResolvedQueryRequest[R](q: R, alg: StreamQuery[R])

  sealed abstract class StreamRequestParser[A] {
    case class QueryRequest[Q](request: Q, resolver: RequestResolver[Q])
    def parse(
        resumingAtOffset: Boolean,
        decoder: ApiJsonDecoder,
        jv: JsValue,
        jwt: Jwt,
    )(implicit
        lc: LoggingContextOf[InstanceUUID]
    ): Future[Error \/ (_ <: QueryRequest[_])]
  }

  trait RequestResolver[Q] {
    def resolve(
        req: Q,
        resolveContractTypeId: PackageService.ResolveContractTypeId,
        jwt: Jwt,
    )(implicit
        lc: LoggingContextOf[InstanceUUID]
    ): Future[Error \/ ResolvedQueryRequest[_]]
  }

  sealed trait StreamQuery[R] {

    /** Extra data on success of a predicate. */
    type Positive

    def removePhantomArchives(resolvedRequest: R): Option[Set[ContractId]]

    private[WebSocketService] def predicate(
        resolvedRequest: R,
        resolveContractTypeId: PackageService.ResolveContractTypeId,
        jwt: Jwt,
    )(implicit
        lc: LoggingContextOf[InstanceUUID]
    ): Future[StreamPredicate[Positive]]

    def renderCreatedMetadata(p: Positive): Map[String, JsValue]

    def acsRequest(
        maybePrefix: Option[StartingOffset],
        resolvedRequest: R,
    ): Option[R]

    /** Perform any necessary adjustment to the request based on the prefix
      */
    def adjustRequest(
        prefix: Option[StartingOffset],
        resolvedRequest: R,
    ): R

    /** Specify the offset from which the live part of the query should start
      */
    def liveStartingOffset(
        prefix: Option[StartingOffset],
        resolvedRequest: R,
    ): Option[StartingOffset]
  }

  final case class ResolvedSearchForeverRequest(
      resolvedQuery: ResolvedQuery,
      queriesWithPos: NonEmpty[List[(ResolvedSearchForeverQuery, Int)]],
      unresolved: Set[ContractTypeId.RequiredPkg],
  )

  final case class ResolvedSearchForeverQuery(
      resolvedQuery: ResolvedQuery,
      offset: Option[Offset],
  )

  implicit def SearchForeverRequestWithStreamQuery(implicit
      ec: ExecutionContext
  ): StreamRequestParser[SearchForeverRequest] =
    new StreamRequestParser[SearchForeverRequest]
      with StreamQuery[ResolvedSearchForeverRequest]
      with RequestResolver[SearchForeverRequest] {
      type Positive = NonEmptyList[Int]

      override def parse(
          resumingAtOffset: Boolean,
          decoder: ApiJsonDecoder,
          jv: JsValue,
          jwt: Jwt,
      )(implicit
          lc: LoggingContextOf[InstanceUUID]
      ) = {
        import JsonProtocol.*
        Future.successful(
          SprayJson
            .decode[SearchForeverRequest](jv)
            .liftErr[Error](InvalidUserInput.apply)
            .map(QueryRequest(_, this))
        )
      }

      override def resolve(
          req: SearchForeverRequest,
          resolveContractTypeId: PackageService.ResolveContractTypeId,
          jwt: Jwt,
      )(implicit
          lc: LoggingContextOf[InstanceUUID]
      ): Future[Error \/ ResolvedQueryRequest[_]] = {
        import scalaz.syntax.foldable.*

        def resolveIds(
            sfq: SearchForeverQuery
        ): Future[(Set[ContractTypeRef.Resolved], Set[ContractTypeId.RequiredPkg])] =
          sfq.templateIds.toList.toNEF
            .traverse(x => resolveContractTypeId(jwt)(x).map(_.toOption.flatten.toLeft(x)))
            .map(
              _.toSet.partitionMap(
                identity[
                  Either[ContractTypeRef.Resolved, ContractTypeId.RequiredPkg]
                ]
              )
            )

        def query(
            sfq: SearchForeverQuery,
            pos: Int,
        ): Future[
          Unsupported \/ (
              ResolvedSearchForeverQuery,
              Int,
              Set[ContractTypeId.RequiredPkg],
          )
        ] = for {
          partitionedResolved <- resolveIds(sfq)
          (resolved, unresolved) = partitionedResolved
          res = ResolvedQuery(resolved).map { rq =>
            (ResolvedSearchForeverQuery(rq, sfq.offset), pos, unresolved)
          }
        } yield res

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
                    Set[ContractTypeId.RequiredPkg],
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
          jwt: Jwt,
      )(implicit
          lc: LoggingContextOf[InstanceUUID]
      ): Future[StreamPredicate[Positive]] = {

        import util.Collections.*

        val indexedOffsets: Vector[Option[Offset]] =
          request.queriesWithPos.map { case (q, _) => q.offset }.toVector

        def matchesOffset(queryIndex: Int, maybeEventOffset: Option[Offset]): Boolean = {
          import Offset.`Offset ordering`
          import scalaz.syntax.order.*
          val matches =
            for {
              queryOffset <- indexedOffsets(queryIndex)
              eventOffset <- maybeEventOffset
            } yield eventOffset > queryOffset
          matches.getOrElse(true)
        }

        def fn(
            q: Map[ContractTypeId.ResolvedPkgId, NonEmptyList[(Int, Int)]]
        )(
            a: ActiveContract.ResolvedCtTyId[LfV],
            o: Option[Offset],
        ): Option[Positive] =
          q.get(a.templateId).flatMap { preds =>
            preds.collect(Function unlift { case (ix, pos) =>
              matchesOffset(ix, o).option(pos)
            })
          }

        def query(
            rsfq: ResolvedSearchForeverQuery,
            pos: Int,
            ix: Int,
        ): NonEmpty[Map[ContractTypeId.ResolvedPkgId, NonEmptyList[
          (Int, Int)
        ]]] =
          rsfq.resolvedQuery.resolved.flatMap(_.allPkgIds).map(_ -> NonEmptyList(ix -> pos)).toMap

        val q = {
          import scalaz.syntax.foldable1.*
          request.queriesWithPos.zipWithIndex // index is used to ensure matchesOffset works properly
            .map { case ((q, pos), ix) => (q, pos, ix) }
            .toNEF
            .foldMap1((query _).tupled)
        }

        Future.successful(
          StreamPredicate(
            request.resolvedQuery,
            request.unresolved,
            fn(q),
          )
        )
      }

      override def renderCreatedMetadata(p: Positive) =
        Map {
          import JsonProtocol.*
          "matchedQueries" -> p.toJson
        }

      override def acsRequest(
          maybePrefix: Option[StartingOffset],
          request: ResolvedSearchForeverRequest,
      ): Option[ResolvedSearchForeverRequest] = {
        val withoutOffset =
          NonEmpty.from(request.queriesWithPos.filter { case (q, _) => q.offset.isEmpty })

        withoutOffset.map(
          ResolvedSearchForeverRequest(request.resolvedQuery, _, request.unresolved)
        )
      }

      override def adjustRequest(
          prefix: Option[StartingOffset],
          request: ResolvedSearchForeverRequest,
      ): ResolvedSearchForeverRequest =
        prefix.fold(request)(prefix =>
          request.copy(
            queriesWithPos = request.queriesWithPos.map {
              _ leftMap (q => q.copy(offset = q.offset.orElse(Some(prefix.offset))))
            }
          )
        )

      import scalaz.syntax.foldable1.*
      import Offset.`Offset ordering`
      import scalaz.std.option.optionOrder

      // This is called after `adjustRequest` already filled in the blank offsets
      override def liveStartingOffset(
          prefix: Option[StartingOffset],
          request: ResolvedSearchForeverRequest,
      ): Option[StartingOffset] =
        request.queriesWithPos
          .map { case (q, _) => q.offset }
          .toNEF
          .minimumBy1(identity)
          .map(StartingOffset(_))

    }

  final case class ResolvedContractKeyStreamRequest[C, V](
      resolvedQuery: ResolvedQuery,
      list: NonEmptyList[ContractKeyStreamRequest[C, V]],
      q: NonEmpty[Map[ContractTypeRef.Resolved, NonEmpty[Set[V]]]],
      unresolved: Set[ContractTypeId.RequiredPkg],
  )

  implicit def EnrichedContractKeyWithStreamQuery(implicit
      ec: ExecutionContext
  ): StreamRequestParser[ContractKeyStreamRequest[_, _]] =
    new StreamRequestParser[ContractKeyStreamRequest[_, _]] {
      import JsonProtocol.*

      override def parse(
          resumingAtOffset: Boolean,
          decoder: ApiJsonDecoder,
          jv: JsValue,
          jwt: Jwt,
      )(implicit
          lc: LoggingContextOf[InstanceUUID]
      ) = {
        type NelCKRH[Hint, V] = NonEmptyList[ContractKeyStreamRequest[Hint, V]]
        def go[Hint](
            resolver: RequestResolver[NelCKRH[Hint, LfV]]
        )(implicit ev: JsonReader[NelCKRH[Hint, JsValue]]) =
          for {
            as <- either[Future, Error, NelCKRH[Hint, JsValue]](
              SprayJson
                .decode[NelCKRH[Hint, JsValue]](jv)
                .liftErr[Error](InvalidUserInput.apply)
            )
            bs <- rightT {
              as.map(a => decodeWithFallback(decoder, a, jwt)).sequence
            }
          } yield QueryRequest(bs, resolver)
        if (resumingAtOffset) go(ResumingEnrichedContractKeyWithStreamQuery())
        else go(InitialEnrichedContractKeyWithStreamQuery())
      }.run

      private def decodeWithFallback[Hint](
          decoder: ApiJsonDecoder,
          a: ContractKeyStreamRequest[Hint, JsValue],
          jwt: Jwt,
      )(implicit
          lc: LoggingContextOf[InstanceUUID]
      ): Future[ContractKeyStreamRequest[Hint, LfValue]] =
        decoder
          .decodeUnderlyingValuesToLf(a, jwt)
          .run
          .map(
            _.valueOr(_ => a.map(_ => com.digitalasset.daml.lf.value.Value.ValueUnit))
          ) // unit will not match any key
    }

  private[this] sealed abstract class EnrichedContractKeyWithStreamQuery[Cid](implicit
      ec: ExecutionContext
  ) extends StreamQuery[ResolvedContractKeyStreamRequest[Cid, LfV]]
      with RequestResolver[NonEmptyList[ContractKeyStreamRequest[Cid, LfV]]] {
    type Positive = Unit

    protected type CKR[+V] = ContractKeyStreamRequest[Cid, V]

    override def resolve(
        request: NonEmptyList[ContractKeyStreamRequest[Cid, LfV]],
        resolveContractTypeId: PackageService.ResolveContractTypeId,
        jwt: Jwt,
    )(implicit
        lc: LoggingContextOf[InstanceUUID]
    ): Future[Error \/ ResolvedQueryRequest[_]] = {
      def getQ[K, V](resolvedWithKey: NonEmpty[Set[(K, V)]]): NonEmpty[Map[K, NonEmpty[Set[V]]]] =
        resolvedWithKey.groupMap(_._1)(_._2)

      request.toList
        .traverse { (x: CKR[LfV]) =>
          resolveContractTypeId(jwt)(x.ekey.templateId)
            .map(_.toOption.flatten.map((_, x.ekey.key)).toLeft(x.ekey.templateId))
        }
        .map { resolveTries =>
          val (resolvedWithKey, unresolved) = resolveTries
            .toSet[Either[(ContractTypeRef.Resolved, LfV), RequiredPkg]]
            .partitionMap(identity)
          for {
            resolvedWithKey <- (NonEmpty from resolvedWithKey
              toRightDisjunction InvalidUserInput(ResolvedQuery.CannotBeEmpty.errorMsg))
            q = getQ(resolvedWithKey)
            rq <- ResolvedQuery(q.keySet)
              .leftMap(unsupported => InvalidUserInput(unsupported.errorMsg))
          } yield ResolvedQueryRequest(
            ResolvedContractKeyStreamRequest(rq, request, q, unresolved),
            this,
          )
        }
    }

    override private[WebSocketService] def predicate(
        resolvedRequest: ResolvedContractKeyStreamRequest[Cid, LfV],
        resolveContractTypeId: PackageService.ResolveContractTypeId,
        jwt: Jwt,
    )(implicit
        lc: LoggingContextOf[InstanceUUID]
    ): Future[StreamPredicate[Positive]] = {
      def fn(
          q: Map[ContractTypeId.ResolvedPkgId, NonEmpty[Set[LfV]]]
      ): (ActiveContract.ResolvedCtTyId[LfV], Option[Offset]) => Option[Positive] = { (a, _) =>
        a.key match {
          case None => None
          case Some(k) =>
            if (q.get(a.templateId).exists(_ contains k)) Some(()) else None
        }
      }

      Future.successful(
        StreamPredicate[Positive](
          resolvedRequest.resolvedQuery,
          resolvedRequest.unresolved,
          fn(resolvedRequest.q.flatMap { case (k, v) => k.allPkgIds.map(_ -> v) }.forgetNE.toMap),
        )
      )
    }

    override def renderCreatedMetadata(p: Unit) = Map.empty

    override def adjustRequest(
        prefix: Option[StartingOffset],
        request: ResolvedContractKeyStreamRequest[Cid, LfV],
    ): ResolvedContractKeyStreamRequest[Cid, LfV] = request

    override def acsRequest(
        maybePrefix: Option[StartingOffset],
        request: ResolvedContractKeyStreamRequest[Cid, LfV],
    ): Option[ResolvedContractKeyStreamRequest[Cid, LfV]] =
      maybePrefix.cata(_ => None, Some(request))

    override def liveStartingOffset(
        prefix: Option[StartingOffset],
        request: ResolvedContractKeyStreamRequest[Cid, LfV],
    ): Option[StartingOffset] = prefix

  }

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
  ) extends EnrichedContractKeyWithStreamQuery[Option[Option[ContractId]]] {
    override def removePhantomArchives(
        request: ResolvedContractKeyStreamRequest[Option[Option[ContractId]], LfV]
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
    decoder: ApiJsonDecoder,
    wsConfig: Option[WebsocketConfig],
    val loggerFactory: NamedLoggerFactory,
)(implicit mat: Materializer, ec: ExecutionContext)
    extends NamedLogging
    with NoTracing {

  import ContractsService.buildTransactionFilter
  import WebSocketService.*
  import com.daml.scalautil.Statement.discard
  import util.ErrorOps.*
  import com.digitalasset.canton.http.json.JsonProtocol.*

  private val config = wsConfig.getOrElse(WebsocketConfig())

  private val numConns = new java.util.concurrent.atomic.AtomicInteger(0)

  def transactionMessageHandler[A: StreamRequestParser](
      jwt: Jwt,
      jwtPayload: JwtPayload,
  )(implicit
      lc: LoggingContextOf[InstanceUUID],
      metrics: HttpApiMetrics,
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
      metrics: HttpApiMetrics,
  ): Flow[A, A, NotUsed] =
    Flow[A]
      .watchTermination() { (_, future) =>
        val afterInc = numConns.incrementAndGet()
        metrics.websocketRequestCounter.inc()
        logger.info(
          s"New websocket client has connected, current number of clients:$afterInc, ${lc.makeString}"
        )
        future onComplete { td =>
          def msg = td.fold(
            ex => s"interrupted on Failure: ${ex.getMessage}. remaining",
            _ => "has disconnected. Current",
          )
          val afterDec = numConns.decrementAndGet()
          metrics.websocketRequestCounter.dec()
          logger.info(s"Websocket client $msg number of clients: $afterDec, ${lc.makeString}")
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
      .flatMapMerge(
        2, // 2 streams max, the 2nd is to be able to send an error back
        _.map { case (offPrefix, rq: ResolvedQueryRequest[q]) =>
          implicit val SQ: StreamQuery[q] = rq.alg
          getTransactionSourceForParty[q](
            jwt,
            jwtPayload.parties,
            offPrefix,
            rq.q: q,
          ) via logTermination(logger, "getTransactionSourceForParty")
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
        SprayJson.parse(m.text).liftErr(InvalidUserInput.apply)
      }
    case bm: BinaryMessage =>
      // ignore binary messages but drain content to avoid the stream being clogged
      discard(bm.dataStream.runWith(Sink.ignore))
      Future successful -\/(
        InvalidUserInput(
          "Invalid request. Expected a single TextMessage with JSON payload, got BinaryMessage"
        )
      )
  }

  private[this] def fetchAndPreFilterAcs[Positive](
      predicate: StreamPredicate[Positive],
      jwt: Jwt,
      parties: PartySet,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Future[Source[StepAndErrors[Positive, JsValue], NotUsed]] =
    Future.successful {
      contractsService
        .liveAcsAsInsertDeleteStepSource(
          jwt,
          buildTransactionFilter(parties, predicate.resolvedQuery),
        )
        .via(
          convertFilterContracts(
            predicate.resolvedQuery,
            predicate.fn,
          )
        )
    }

  // simple alias to avoid passing in the class parameters
  private[this] def queryPredicate[A](
      request: A,
      jwt: Jwt,
  )(implicit
      lc: LoggingContextOf[InstanceUUID],
      Q: StreamQuery[A],
  ): Future[StreamPredicate[Q.Positive]] =
    Q.predicate(request, resolveContractTypeId, jwt)

  private def getTransactionSourceForParty[A](
      jwt: Jwt,
      parties: PartySet,
      offPrefix: Option[StartingOffset],
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
        .map(queryPredicate(_, jwt))
        .sequence

    def liveFrom(resolvedQuery: ResolvedQuery)(
        acsEnd: Option[StartingOffset]
    ): Future[Source[StepAndErrors[Q.Positive, JsValue], NotUsed]] = {
      val shiftedRequest = Q.adjustRequest(acsEnd, request)
      val liveStartingOffset = Q.liveStartingOffset(acsEnd, shiftedRequest)

      // Produce the predicate that is going to be applied to the incoming transaction stream
      // We need to apply this to the request with all the offsets shifted so that each stream
      // can filter out anything from liveStartingOffset to the query-specific offset
      queryPredicate(shiftedRequest, jwt).map { case StreamPredicate(_, _, fn) =>
        contractsService
          .insertDeleteStepSource(
            jwt,
            buildTransactionFilter(parties, resolvedQuery),
            liveStartingOffset,
            Terminates.Never,
          )
          .via(logTermination(logger, "insertDeleteStepSource with ACS"))
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
        unresolved: Set[RequiredPkg],
        fn: (ActiveContract.ResolvedCtTyId[LfV], Option[Offset]) => Option[Q.Positive],
    ) =
      acsPred
        .flatMap(
          _.flatMap { (vp: StreamPredicate[Q.Positive]) =>
            Some(fetchAndPreFilterAcs(vp, jwt, parties))
          }.cata(
            _.map { acsAndLiveMarker =>
              acsAndLiveMarker
                .mapAsync(1) {
                  case acs @ StepAndErrors(_, Acs(_), _) if acs.nonEmpty =>
                    Future.successful(Source.single(acs))
                  case StepAndErrors(_, Acs(_), _) =>
                    Future.successful(Source.empty)
                  case liveBegin @ StepAndErrors(_, LiveBegin(offset), _) =>
                    val acsEnd = offset.toOption.map(StartingOffset(_))
                    liveFrom(resolvedQuery)(acsEnd).map(it => Source.single(liveBegin) ++ it)
                  case txn @ StepAndErrors(_, Txn(_, offset), _) =>
                    val acsEnd = Some(StartingOffset(offset))
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
                    buildTransactionFilter(parties, resolvedQuery),
                    liveStartingOffset,
                    Terminates.Never,
                  )
                  .via(logTermination(logger, "insertDeleteStepSource without ACS"))
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
        queryPredicate(request, jwt).flatMap { pred =>
          processResolved(pred.resolvedQuery, pred.unresolved, pred.fn)
        }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def emitOffsetTicksAndFilterOutEmptySteps[Pos](
      offset: Option[StartingOffset]
  ): Flow[StepAndErrors[Pos, JsValue], StepAndErrors[Pos, JsValue], NotUsed] = {

    val zero = (
      offset.map(o => AbsoluteBookmark(o.offset)): Option[BeginBookmark[Offset]],
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

  private def offsetTick[Pos](offset: BeginBookmark[Offset]) =
    StepAndErrors[Pos, JsValue](Seq.empty, LiveBegin(offset), loggerFactory)

  private def removePhantomArchives[A, B](remove: Option[Set[ContractId]]) =
    remove.cata(removePhantomArchives_[A, B], Flow[StepAndErrors[A, B]])

  private def removePhantomArchives_[A, B](
      initialState: Set[ContractId]
  ): Flow[StepAndErrors[A, B], StepAndErrors[A, B], NotUsed] = {
    import ContractStreamStep.{LiveBegin, Txn, Acs}
    Flow[StepAndErrors[A, B]]
      .scan((Tag unsubst initialState, Option.empty[StepAndErrors[A, B]])) {
        case ((s0, _), a0 @ StepAndErrors(_, Txn(idstep, _), _)) =>
          val newInserts: Vector[String] =
            ContractId.unsubst(idstep.inserts.map(_._1.contractId))
          val (deletesToEmit, deletesToHold) = s0 partition idstep.deletes.keySet
          val s1: Set[String] = deletesToHold ++ newInserts
          val a1 = a0.copy(step = a0.step.mapDeletes(_.view.filterKeys(deletesToEmit).toMap))

          (s1, if (a1.nonEmpty) Some(a1) else None)

        case ((deletesToHold, _), a0 @ StepAndErrors(_, Acs(inserts), _)) =>
          val newInserts: Vector[String] = ContractId.unsubst(inserts.map(_._1.contractId))
          val s1: Set[String] = deletesToHold ++ newInserts
          (s1, Some(a0))

        case ((s0, _), a0 @ StepAndErrors(_, LiveBegin(_), loggerFactory)) =>
          (s0, Some(a0))
      }
      .collect { case (_, Some(x)) => x }
  }

  def wsErrorMessage(error: Error)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): TextMessage.Strict =
    wsMessage(SprayJson.encodeUnsafe(errorResponse(error, logger)))

  def wsMessage(jsVal: JsValue): TextMessage.Strict =
    TextMessage(jsVal.compactPrint)

  private def convertFilterContracts[Pos](
      resolvedQuery: ResolvedQuery,
      fn: (ActiveContract.ResolvedCtTyId[LfV], Option[Offset]) => Option[Pos],
  ): Flow[ContractStreamStep.LAV1, StepAndErrors[Pos, JsValue], NotUsed] =
    Flow
      .fromFunction { (step: ContractStreamStep.LAV1) =>
        val (aerrors, errors, dstep) = step.partitionBimap(
          ae =>
            ArchivedContract
              .fromLedgerApi(resolvedQuery, ae)
              .liftErr(ServerError.fromMsg),
          ce =>
            ActiveContract
              .fromLedgerApi(ActiveContract.ExtractAs(resolvedQuery), ce)
              .liftErr(ServerError.fromMsg)
              .flatMap(_.traverse(apiValueToLfValue).liftErr(ServerError.fromMsg)),
        )(Seq)
        StepAndErrors(
          errors ++ aerrors,
          dstep mapInserts { (inserts: Vector[ActiveContract.ResolvedCtTyId[LfV]]) =>
            inserts.flatMap { ac =>
              fn(ac, dstep.bookmark.flatMap(_.toOption)).map((ac, _)).toList
            }
          },
          loggerFactory,
        )
      }
      .via(conflation)
      .map(_ mapLfv lfValueToJsValue)

  private def reportUnresolvedTemplateIds(
      unresolved: Set[ContractTypeId.RequiredPkg]
  ): Source[JsValue, NotUsed] =
    if (unresolved.isEmpty) Source.empty
    else {
      Source.single(
        AsyncWarningsWrapper(UnknownTemplateIds(unresolved.toList)).toJson
      )
    }
}
