// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.stream.scaladsl.{
  Broadcast,
  Concat,
  Flow,
  GraphDSL,
  Keep,
  Partition,
  RunnableGraph,
  Sink,
  SinkQueueWithCancel,
  Source,
}
import akka.stream.{ClosedShape, FanOutShape2, FlowShape, Graph, Materializer}
import com.daml.http.Statement.discard
import com.daml.http.dbbackend.ContractDao.StaleOffsetException
import com.daml.http.dbbackend.{ContractDao, Queries}
import com.daml.http.dbbackend.Queries.{DBContract, SurrogateTpId}
import com.daml.http.domain.TemplateId
import com.daml.http.LedgerClientJwt.Terminates
import com.daml.http.util.ApiValueToLfValueConverter.apiValueToLfValue
import com.daml.http.json.JsonProtocol.LfValueDatabaseCodec.{
  apiValueToJsValue => lfValueToDbJsValue,
}
import com.daml.http.util.IdentifierConverters.apiIdentifier
import util.{AbsoluteBookmark, BeginBookmark, ContractStreamStep, InsertDeleteStep, LedgerBegin}
import com.daml.util.ExceptionOps._
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.{v1 => lav1}
import doobie.free.connection
import doobie.free.connection.ConnectionIO
import doobie.postgres.sqlstate.{class23 => postgres_class23}
import scalaz.std.vector._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.functor._
import scalaz.syntax.std.option._
import scalaz.{-\/, \/, \/-}
import spray.json.{JsNull, JsValue}
import com.typesafe.scalalogging.StrictLogging
import scalaz.Liskov.<~<

import scala.concurrent.{ExecutionContext, Future}

private class ContractsFetch(
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    getTermination: LedgerClientJwt.GetTermination,
    lookupType: query.ValuePredicate.TypeLookup,
)(implicit dblog: doobie.LogHandler)
    extends StrictLogging {

  import ContractsFetch._

  private val retrySqlStates: Set[String] = Set(
    postgres_class23.UNIQUE_VIOLATION.value,
    StaleOffsetException.SqlState,
  )

  def fetchAndPersist(
      jwt: Jwt,
      party: domain.Party,
      templateIds: List[domain.TemplateId.RequiredPkg],
  )(
      implicit ec: ExecutionContext,
      mat: Materializer,
  ): ConnectionIO[List[BeginBookmark[domain.Offset]]] = {
    import cats.instances.list._, cats.syntax.traverse._, doobie.implicits._, doobie.free.{
      connection => fc,
    }
    // we can fetch for all templateIds on a single acsFollowingAndBoundary
    // by comparing begin offsets; however this is trickier so we don't do it
    // right now -- Stephen / Leo
    connectionIOFuture(getTermination(jwt)) flatMap {
      _ cata (absEnd =>
        templateIds.traverse {
          fetchAndPersist(jwt, party, absEnd, _)
      }, fc.pure(templateIds map (_ => LedgerBegin)))
    }

  }

  private[this] def fetchAndPersist(
      jwt: Jwt,
      party: domain.Party,
      absEnd: Terminates.AtAbsolute,
      templateId: domain.TemplateId.RequiredPkg,
  )(
      implicit ec: ExecutionContext,
      mat: Materializer,
  ): ConnectionIO[BeginBookmark[domain.Offset]] = {

    import doobie.implicits._

    def loop(maxAttempts: Int): ConnectionIO[BeginBookmark[domain.Offset]] = {
      logger.debug(s"contractsIo, maxAttempts: $maxAttempts")
      contractsIo_(jwt, party, absEnd, templateId).exceptSql {
        case e if maxAttempts > 0 && retrySqlStates(e.getSQLState) =>
          logger.debug(s"contractsIo, exception: ${e.description}, state: ${e.getSQLState}")
          connection.rollback flatMap (_ => loop(maxAttempts - 1))
        case e @ _ =>
          logger.error(s"contractsIo3 exception: ${e.description}, state: ${e.getSQLState}")
          connection.raiseError(e)
      }
    }

    loop(5)
  }

  private def contractsIo_(
      jwt: Jwt,
      party: domain.Party,
      absEnd: Terminates.AtAbsolute,
      templateId: domain.TemplateId.RequiredPkg,
  )(implicit ec: ExecutionContext, mat: Materializer): ConnectionIO[BeginBookmark[domain.Offset]] =
    for {
      offset0 <- ContractDao.lastOffset(party, templateId)
      ob0 = offset0.cata(AbsoluteBookmark(_), LedgerBegin)
      offset1 <- contractsFromOffsetIo(jwt, party, templateId, ob0, absEnd)
      _ = logger.debug(s"contractsFromOffsetIo($jwt, $party, $templateId, $ob0): $offset1")
    } yield offset1

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def prepareCreatedEventStorage(
      ce: lav1.event.CreatedEvent,
  ): Exception \/ PreInsertContract = {
    import scalaz.syntax.traverse._
    import scalaz.std.option._
    for {
      ac <- domain.ActiveContract fromLedgerApi ce leftMap (
          de =>
            new IllegalArgumentException(s"contract ${ce.contractId}: ${de.shows}"),
      )
      lfKey <- ac.key.traverse(apiValueToLfValue).leftMap(_.cause)
      lfArg <- apiValueToLfValue(ac.payload) leftMap (_.cause)
    } yield
      DBContract(
        contractId = ac.contractId.unwrap,
        templateId = ac.templateId,
        key = lfKey.cata(lfValueToDbJsValue, JsNull),
        payload = lfValueToDbJsValue(lfArg),
        signatories = ac.signatories,
        observers = ac.observers,
        agreementText = ac.agreementText,
      )
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def jsonifyInsertDeleteStep(
      a: InsertDeleteStep[Any, lav1.event.CreatedEvent],
  ): InsertDeleteStep[Unit, PreInsertContract] =
    a.leftMap(_ => ())
      .mapPreservingIds(prepareCreatedEventStorage(_) valueOr (e => throw e))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def contractsFromOffsetIo(
      jwt: Jwt,
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg,
      offset: BeginBookmark[domain.Offset],
      absEnd: Terminates.AtAbsolute,
  )(
      implicit ec: ExecutionContext,
      mat: Materializer,
  ): ConnectionIO[BeginBookmark[domain.Offset]] = {

    val graph = RunnableGraph.fromGraph(
      GraphDSL.create(
        Sink.queue[ConnectionIO[Unit]](),
        Sink.last[BeginBookmark[String]],
      )(Keep.both) { implicit builder => (acsSink, offsetSink) =>
        import GraphDSL.Implicits._

        val txnK = getCreatesAndArchivesSince(
          jwt,
          transactionFilter(party, List(templateId)),
          _: lav1.ledger_offset.LedgerOffset,
          absEnd,
        )

        // include ACS iff starting at LedgerBegin
        val (idses, lastOff) = offset match {
          case LedgerBegin =>
            val stepsAndOffset = builder add acsFollowingAndBoundary(txnK)
            stepsAndOffset.in <~ getActiveContracts(
              jwt,
              transactionFilter(party, List(templateId)),
              true,
            )
            (stepsAndOffset.out0, stepsAndOffset.out1)

          case AbsoluteBookmark(_) =>
            val stepsAndOffset = builder add transactionsFollowingBoundary(txnK)
            stepsAndOffset.in <~ Source.single(domain.Offset.tag.unsubst(offset))
            (
              (stepsAndOffset: FanOutShape2[_, ContractStreamStep.LAV1, _]).out0,
              stepsAndOffset.out1)
        }

        val transactInsertsDeletes = Flow
          .fromFunction(jsonifyInsertDeleteStep)
          .conflate(_ append _)
          .map(insertAndDelete)

        idses.map(_.toInsertDelete) ~> transactInsertsDeletes ~> acsSink
        lastOff ~> offsetSink

        ClosedShape
      },
    )

    val (acsQueue, lastOffsetFuture) = graph.run()

    for {
      _ <- sinkCioSequence_(acsQueue)
      offset0 <- connectionIOFuture(lastOffsetFuture)
      offsetOrError <- offset0 match {
        case AbsoluteBookmark(str) =>
          val newOffset = domain.Offset(str)
          ContractDao
            .updateOffset(party, templateId, newOffset, offset.toOption)
            .map(_ => AbsoluteBookmark(newOffset))
        case LedgerBegin =>
          connection.pure(LedgerBegin)
      }
    } yield offsetOrError
  }
}

private[http] object ContractsFetch {

  type PreInsertContract = DBContract[TemplateId.RequiredPkg, JsValue, JsValue, Seq[domain.Party]]

  def partition[A, B]: Graph[FanOutShape2[A \/ B, A, B], NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val split = b.add(Partition[A \/ B](2, {
        case -\/(_) => 0
        case \/-(_) => 1
      }))
      val as = b.add(Flow[A \/ B].collect { case -\/(a) => a })
      val bs = b.add(Flow[A \/ B].collect { case \/-(b) => b })
      discard { split ~> as }
      discard { split ~> bs }
      new FanOutShape2(split.in, as.out, bs.out)
    }

  def project2[A, B]: Graph[FanOutShape2[(A, B), A, B], NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val split = b add Broadcast[(A, B)](2)
      val left = b add Flow.fromFunction((_: (A, B))._1)
      val right = b add Flow.fromFunction((_: (A, B))._2)
      discard { split ~> left }
      discard { split ~> right }
      new FanOutShape2(split.in, left.out, right.out)
    }

  def last[A](ifEmpty: A): Flow[A, A, NotUsed] =
    Flow[A].fold(ifEmpty)((_, later) => later)

  /** Plan inserts, deletes from an in-order batch of create/archive events. */
  private def partitionInsertsDeletes(
      txes: Traversable[lav1.event.Event],
  ): InsertDeleteStep.LAV1 = {
    val csb = Vector.newBuilder[lav1.event.CreatedEvent]
    val asb = Map.newBuilder[String, lav1.event.ArchivedEvent]
    import lav1.event.Event
    import Event.Event._
    txes foreach {
      case Event(Created(c)) => discard { csb += c }
      case Event(Archived(a)) => discard { asb += ((a.contractId, a)) }
      case Event(Empty) => () // nonsense
    }
    val as = asb.result()
    InsertDeleteStep(csb.result() filter (ce => !as.contains(ce.contractId)), as)
  }

  object GraphExtensions {
    implicit final class `Graph FOS2 funs`[A, Y, Z, M](
        private val g: Graph[FanOutShape2[A, Y, Z], M],
    ) extends AnyVal {
      private def divertToMat[N, O](oz: Sink[Z, N])(mat: (M, N) => O): Flow[A, Y, O] =
        Flow fromGraph GraphDSL.create(g, oz)(mat) { implicit b => (gs, zOut) =>
          import GraphDSL.Implicits._
          gs.out1 ~> zOut
          new FlowShape(gs.in, gs.out0)
        }

      /** Several of the graphs here have a second output guaranteed to deliver only one value.
        * This turns such a graph into a flow with the value materialized.
        */
      def divertToHead(implicit noM: M <~< NotUsed): Flow[A, Y, Future[Z]] =
        divertToMat(Sink.head)(Keep.right[M, Future[Z]])
    }
  }

  /** Like `acsAndBoundary`, but also include the events produced by `transactionsSince`
    * after the ACS's last offset, terminating with the last offset of the last transaction,
    * or the ACS's last offset if there were no transactions.
    */
  private[http] def acsFollowingAndBoundary(
      transactionsSince: lav1.ledger_offset.LedgerOffset => Source[Transaction, NotUsed],
  ): Graph[
    FanOutShape2[
      lav1.active_contracts_service.GetActiveContractsResponse,
      ContractStreamStep.LAV1,
      BeginBookmark[String]],
    NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      import ContractStreamStep.{LiveBegin, Acs}
      type Off = BeginBookmark[String]
      val acs = b add acsAndBoundary
      val dupOff = b add Broadcast[Off](2)
      val liveStart = Flow fromFunction { off: Off =>
        LiveBegin(domain.Offset.tag.subst(off))
      }
      val txns = b add transactionsFollowingBoundary(transactionsSince)
      val allSteps = b add Concat[ContractStreamStep.LAV1](3)
      // format: off
      discard { dupOff <~ acs.out1 }
      discard {           acs.out0.map(ces => Acs(ces.toVector)) ~> allSteps }
      discard { dupOff       ~> liveStart                        ~> allSteps }
      discard {                      txns.out0                   ~> allSteps }
      discard { dupOff            ~> txns.in }
      // format: on
      new FanOutShape2(acs.in, allSteps.out, txns.out1)
    }

  /** Interpreting the transaction stream so it conveniently depends on
    * the ACS graph, if desired.  Deliberately matching output shape
    * to `acsFollowingAndBoundary`.
    */
  private[http] def transactionsFollowingBoundary(
      transactionsSince: lav1.ledger_offset.LedgerOffset => Source[Transaction, NotUsed],
  ): Graph[
    FanOutShape2[
      BeginBookmark[String],
      ContractStreamStep.Txn.LAV1,
      BeginBookmark[String],
    ],
    NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      type Off = BeginBookmark[String]
      val dupOff = b add Broadcast[Off](2)
      val mergeOff = b add Concat[Off](2)
      val txns = Flow[Off]
        .flatMapConcat(off => transactionsSince(domain.Offset.tag.subst(off).toLedgerApi))
        .map(transactionToInsertsAndDeletes)
      val txnSplit = b add project2[ContractStreamStep.Txn.LAV1, domain.Offset]
      val lastOff = b add last(LedgerBegin: Off)
      // format: off
      discard { txnSplit.in <~ txns <~ dupOff }
      discard {                        dupOff                          ~> mergeOff ~> lastOff }
      discard { txnSplit.out1.map(off => AbsoluteBookmark(off.unwrap)) ~> mergeOff }
      // format: on
      new FanOutShape2(dupOff.in, txnSplit.out0, lastOff.out)
    }

  /** Split a series of ACS responses into two channels: one with contracts, the
    * other with a single result, the last offset.
    */
  private[this] def acsAndBoundary: Graph[
    FanOutShape2[
      lav1.active_contracts_service.GetActiveContractsResponse,
      Seq[
        lav1.event.CreatedEvent,
      ],
      BeginBookmark[String]],
    NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      import lav1.active_contracts_service.{GetActiveContractsResponse => GACR}
      val dup = b add Broadcast[GACR](2)
      val acs = b add (Flow fromFunction ((_: GACR).activeContracts))
      val off = b add Flow[GACR]
        .collect { case gacr if gacr.offset.nonEmpty => AbsoluteBookmark(gacr.offset) }
        .via(last(LedgerBegin: BeginBookmark[String]))
      discard { dup ~> acs }
      discard { dup ~> off }
      new FanOutShape2(dup.in, acs.out, off.out)
    }

  private def transactionToInsertsAndDeletes(
      tx: lav1.transaction.Transaction,
  ): (ContractStreamStep.Txn.LAV1, domain.Offset) = {
    val offset = domain.Offset.fromLedgerApi(tx)
    (ContractStreamStep.Txn(partitionInsertsDeletes(tx.events), offset), offset)
  }

  private def surrogateTemplateIds[K <: TemplateId.RequiredPkg](
      ids: Set[K],
  )(implicit log: doobie.LogHandler): ConnectionIO[Map[K, SurrogateTpId]] = {
    import doobie.implicits._, cats.instances.vector._, cats.syntax.functor._,
    cats.syntax.traverse._
    ids.toVector
      .traverse(k =>
        Queries.surrogateTemplateId(k.packageId, k.moduleName, k.entityName) tupleLeft k)
      .map(_.toMap)
  }

  private def sinkCioSequence_[Ign](
      f: SinkQueueWithCancel[doobie.ConnectionIO[Ign]],
  )(implicit ec: ExecutionContext): doobie.ConnectionIO[Unit] = {
    import doobie.ConnectionIO
    import doobie.free.{connection => fconn}
    def go(): ConnectionIO[Unit] = {
      val next = f.pull()
      connectionIOFuture(next)
        .flatMap(_.cata(cio => cio flatMap (_ => go()), fconn.pure(())))
    }
    fconn.handleErrorWith(go(), t => {
      f.cancel()
      fconn.raiseError(t)
    })
  }

  private def connectionIOFuture[A](
      fa: Future[A],
  )(implicit ec: ExecutionContext): doobie.ConnectionIO[A] =
    doobie.free.connection.async[A](k => fa.onComplete(ta => k(ta.toEither)))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def insertAndDelete(
      step: InsertDeleteStep[Any, PreInsertContract],
  )(implicit log: doobie.LogHandler): ConnectionIO[Unit] = {
    import doobie.implicits._, cats.syntax.functor._
    surrogateTemplateIds(step.inserts.iterator.map(_.templateId).toSet).flatMap { stidMap =>
      import cats.syntax.apply._, cats.instances.vector._, scalaz.std.set._
      import json.JsonProtocol._
      import doobie.postgres.implicits._
      (Queries.deleteContracts(step.deletes.keySet) *>
        Queries.insertContracts(
          step.inserts map (
              dbc =>
                dbc copy (templateId = stidMap getOrElse (dbc.templateId, throw new IllegalStateException(
                  "template ID missing from prior retrieval; impossible",
                )),
                signatories = domain.Party.unsubst(dbc.signatories),
                observers = domain.Party.unsubst(dbc.observers)),
          )
        ))
    }.void
  }

  private def transactionFilter(
      party: domain.Party,
      templateIds: List[TemplateId.RequiredPkg],
  ): lav1.transaction_filter.TransactionFilter = {
    import lav1.transaction_filter._

    val filters =
      if (templateIds.isEmpty) Filters.defaultInstance
      else Filters(Some(lav1.transaction_filter.InclusiveFilters(templateIds.map(apiIdentifier))))

    TransactionFilter(Map(domain.Party.unwrap(party) -> filters))
  }
}
