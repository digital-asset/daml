// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.stream.scaladsl.{
  Broadcast,
  Concat,
  Flow,
  GraphDSL,
  Partition,
  RunnableGraph,
  Sink,
  SinkQueueWithCancel,
  Source
}
import akka.stream.{ClosedShape, FanOutShape2, Graph, Materializer}
import com.digitalasset.http.Statement.discard
import com.digitalasset.http.dbbackend.ContractDao.StaleOffsetException
import com.digitalasset.http.dbbackend.{ContractDao, Queries}
import com.digitalasset.http.dbbackend.Queries.{DBContract, SurrogateTpId}
import com.digitalasset.http.domain.TemplateId
import com.digitalasset.http.util.ApiValueToLfValueConverter.apiValueToLfValue
import com.digitalasset.http.json.JsonProtocol.LfValueDatabaseCodec.{
  apiValueToJsValue => lfValueToDbJsValue
}
import com.digitalasset.http.util.IdentifierConverters.apiIdentifier
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.{v1 => lav1}
import doobie.free.connection
import doobie.free.connection.ConnectionIO
import doobie.postgres.sqlstate.{class23 => postgres_class23}
import scalaz.std.vector._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.functor._
import scalaz.syntax.std.option._
import scalaz.{-\/, Liskov, \/, \/-}
import spray.json.JsValue
import com.typesafe.scalalogging.StrictLogging
import scalaz.Liskov.<~<

import scala.concurrent.{ExecutionContext, Future}

private class ContractsFetch(
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    lookupType: query.ValuePredicate.TypeLookup
)(implicit dblog: doobie.LogHandler)
    extends StrictLogging {

  import ContractsFetch._

  private val retrySqlStates: Set[String] = Set(
    postgres_class23.UNIQUE_VIOLATION.value,
    StaleOffsetException.SqlState
  )

  def contractsIo2(jwt: Jwt, party: domain.Party, templateIds: List[domain.TemplateId.RequiredPkg])(
      implicit ec: ExecutionContext,
      mat: Materializer): ConnectionIO[List[OffsetBookmark[domain.Offset]]] = {
    import cats.instances.list._, cats.syntax.traverse._, doobie.implicits._
    // TODO(Leo/Stephen): can we run this traverse concurrently?
    templateIds.traverse { templateId =>
      contractsIo(jwt, party, templateId)
    }
  }

  def contractsIo(jwt: Jwt, party: domain.Party, templateId: domain.TemplateId.RequiredPkg)(
      implicit ec: ExecutionContext,
      mat: Materializer): ConnectionIO[OffsetBookmark[domain.Offset]] = {

    import doobie.implicits._

    def loop(maxAttempts: Int): ConnectionIO[OffsetBookmark[domain.Offset]] = {
      logger.debug(s"contractsIo, maxAttempts: $maxAttempts")
      contractsIo_(jwt, party, templateId).exceptSql {
        case e if maxAttempts > 0 && retrySqlStates(e.getSQLState) =>
          logger.debug(s"contractsIo, exception: ${e.getMessage}, state: ${e.getSQLState}")
          connection.rollback flatMap (_ => loop(maxAttempts - 1))
        case e @ _ =>
          logger.error(s"contractsIo3 exception: ${e.getMessage}, state: ${e.getSQLState}")
          connection.raiseError(e)
      }
    }

    loop(5)
  }

  private def contractsIo_(
      jwt: Jwt,
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg)(
      implicit ec: ExecutionContext,
      mat: Materializer): ConnectionIO[OffsetBookmark[domain.Offset]] =
    for {
      offset0 <- ContractDao.lastOffset(party, templateId)
      ob0 = offset0.cata(AbsoluteBookmark(_), LedgerBegin)
      offset1 <- contractsFromOffsetIo(jwt, party, templateId, ob0)
      _ = logger.debug(s"contractsFromOffsetIo($jwt, $party, $templateId, $ob0): $offset1")
    } yield offset1

  private def prepareCreatedEventStorage(
      ce: lav1.event.CreatedEvent): Exception \/ PreInsertContract =
    for {
      ac <- domain.ActiveContract fromLedgerApi ce leftMap (de =>
        new IllegalArgumentException(s"contract ${ce.contractId}: ${de.shows}"))
      lfArg <- apiValueToLfValue(ac.argument) leftMap (_.cause)
    } yield
      DBContract(
        contractId = ac.contractId.unwrap,
        templateId = ac.templateId,
        createArguments = lfValueToDbJsValue(lfArg),
        witnessParties = ac.witnessParties)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def jsonifyInsertDeleteStep(
      a: InsertDeleteStep[lav1.event.CreatedEvent]): InsertDeleteStep[PreInsertContract] = {
    import scalaz.syntax.traverse._
    InsertDeleteStep(
      a.inserts traverse prepareCreatedEventStorage fold (e => throw e, identity),
      a.deletes
    )
  }

  private def contractsFromOffsetIo(
      jwt: Jwt,
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg,
      offset: OffsetBookmark[domain.Offset])(
      implicit ec: ExecutionContext,
      mat: Materializer): ConnectionIO[OffsetBookmark[domain.Offset]] = {

    val graph = RunnableGraph.fromGraph(
      GraphDSL.create(
        Sink.queue[ConnectionIO[Unit]](),
        Sink.last[OffsetBookmark[String]]
      )((a, b) => (a, b)) { implicit builder => (acsSink, offsetSink) =>
        import GraphDSL.Implicits._

        val txnK = getCreatesAndArchivesSince(
          jwt,
          transactionFilter(party, List(templateId)),
          _: lav1.ledger_offset.LedgerOffset)

        // include ACS iff starting at LedgerBegin
        val (idses, lastOff) = offset match {
          case LedgerBegin =>
            val stepsAndOffset = builder add acsFollowingAndBoundary(txnK)
            stepsAndOffset.in <~ getActiveContracts(
              jwt,
              transactionFilter(party, List(templateId)),
              true)
            (stepsAndOffset.out0, stepsAndOffset.out1)

          case AbsoluteBookmark(_) =>
            val stepsAndOffset = builder add transactionsFollowingBoundary(txnK)
            stepsAndOffset.in <~ Source.single(domain.Offset.tag.unsubst(offset))
            (stepsAndOffset.out0, stepsAndOffset.out1)
        }

        val transactInsertsDeletes = Flow
          .fromFunction(jsonifyInsertDeleteStep)
          .conflate(_ append _)
          .map(insertAndDelete)

        idses ~> transactInsertsDeletes ~> acsSink
        lastOff ~> offsetSink

        ClosedShape
      })

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
  type Contract = domain.Contract[lav1.value.Value]

  type PreInsertContract = DBContract[TemplateId.RequiredPkg, JsValue, Seq[domain.Party]]

  sealed abstract class OffsetBookmark[+Off] extends Product with Serializable {
    import lav1.ledger_offset.LedgerOffset
    import LedgerOffset.{LedgerBoundary, Value}
    import Value.Boundary
    def toLedgerApi(implicit ev: Off <~< domain.Offset): LedgerOffset =
      this match {
        case AbsoluteBookmark(offset) => domain.Offset.toLedgerApi(ev(offset))
        case LedgerBegin => LedgerOffset(Boundary(LedgerBoundary.LEDGER_BEGIN))
      }

    def toOption: Option[Off] = this match {
      case AbsoluteBookmark(offset) => Some(offset)
      case LedgerBegin => None
    }
  }
  final case class AbsoluteBookmark[+Off](offset: Off) extends OffsetBookmark[Off]
  case object LedgerBegin extends OffsetBookmark[Nothing]

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
      txes: Traversable[lav1.event.Event]): InsertDeleteStep[lav1.event.CreatedEvent] = {
    val csb = Vector.newBuilder[lav1.event.CreatedEvent]
    val asb = Set.newBuilder[String]
    import lav1.event.Event
    import Event.Event._
    txes foreach {
      case Event(Created(c)) => discard { csb += c }
      case Event(Archived(a)) => discard { asb += a.contractId }
      case Event(Empty) => () // nonsense
    }
    val as = asb.result()
    InsertDeleteStep(csb.result() filter (ce => !as.contains(ce.contractId)), as)
  }

  /** Like `acsAndBoundary`, but also include the events produced by `transactionsSince`
    * after the ACS's last offset, terminating with the last offset of the last transaction,
    * or the ACS's last offset if there were no transactions.
    */
  private[http] def acsFollowingAndBoundary(
      transactionsSince: lav1.ledger_offset.LedgerOffset => Source[Transaction, NotUsed]): Graph[
    FanOutShape2[
      lav1.active_contracts_service.GetActiveContractsResponse,
      InsertDeleteStep[lav1.event.CreatedEvent],
      OffsetBookmark[String]],
    NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val acs = b add acsAndBoundary
      val txns = b add transactionsFollowingBoundary(transactionsSince)
      val allSteps = b add Concat[InsertDeleteStep[lav1.event.CreatedEvent]](2)
      discard { acs.out0.map(sce => InsertDeleteStep(sce.toVector, Set.empty)) ~> allSteps }
      discard { allSteps <~ txns.out0 }
      discard { acs.out1 ~> txns.in }
      new FanOutShape2(acs.in, allSteps.out, txns.out1)
    }

  /** Interpreting the transaction stream so it conveniently depends on
    * the ACS graph, if desired.  Deliberately matching output shape
    * to `acsFollowingAndBoundary`.
    */
  private def transactionsFollowingBoundary(
      transactionsSince: lav1.ledger_offset.LedgerOffset => Source[Transaction, NotUsed]): Graph[
    FanOutShape2[
      OffsetBookmark[String],
      InsertDeleteStep[lav1.event.CreatedEvent],
      OffsetBookmark[String]],
    NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      type Off = OffsetBookmark[String]
      val dupOff = b add Broadcast[Off](2)
      val mergeOff = b add Concat[Off](2)
      val txns = Flow[Off]
        .flatMapConcat(off => transactionsSince(domain.Offset.tag.subst(off).toLedgerApi))
        .map(transactionToInsertsAndDeletes)
      val txnSplit = b add project2[InsertDeleteStep[lav1.event.CreatedEvent], domain.Offset]
      val lastOff = b add last(LedgerBegin: Off)
      discard { dupOff ~> txns ~> txnSplit.in }
      discard { dupOff ~> mergeOff ~> lastOff }
      discard { txnSplit.out1.map(off => AbsoluteBookmark(off.unwrap)) ~> mergeOff }
      new FanOutShape2(dupOff.in, txnSplit.out0, lastOff.out)
    }

  /** Split a series of ACS responses into two channels: one with contracts, the
    * other with a single result, the last offset.
    */
  private[this] def acsAndBoundary: Graph[
    FanOutShape2[
      lav1.active_contracts_service.GetActiveContractsResponse,
      Seq[lav1.event.CreatedEvent],
      OffsetBookmark[String]],
    NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      import lav1.active_contracts_service.{GetActiveContractsResponse => GACR}
      val dup = b add Broadcast[GACR](2)
      val acs = b add (Flow fromFunction ((_: GACR).activeContracts))
      val off = b add Flow[GACR]
        .collect { case gacr if gacr.offset.nonEmpty => AbsoluteBookmark(gacr.offset) }
        .via(last(LedgerBegin: OffsetBookmark[String]))
      discard { dup ~> acs }
      discard { dup ~> off }
      new FanOutShape2(dup.in, acs.out, off.out)
    }

  private def transactionToInsertsAndDeletes(tx: lav1.transaction.Transaction)
    : (InsertDeleteStep[lav1.event.CreatedEvent], domain.Offset) = {
    val offset = domain.Offset.fromLedgerApi(tx)
    (partitionInsertsDeletes(tx.events), offset)
  }

  private def surrogateTemplateIds[K <: TemplateId.RequiredPkg](ids: Set[K])(
      implicit log: doobie.LogHandler): ConnectionIO[Map[K, SurrogateTpId]] = {
    import doobie.implicits._, cats.instances.vector._, cats.syntax.functor._,
    cats.syntax.traverse._
    ids.toVector
      .traverse(k =>
        Queries.surrogateTemplateId(k.packageId, k.moduleName, k.entityName) tupleLeft k)
      .map(_.toMap)
  }

  private def sinkCioSequence_[Ign](f: SinkQueueWithCancel[doobie.ConnectionIO[Ign]])(
      implicit ec: ExecutionContext): doobie.ConnectionIO[Unit] = {
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

  private def connectionIOFuture[A](fa: Future[A])(
      implicit ec: ExecutionContext): doobie.ConnectionIO[A] =
    doobie.free.connection.async[A](k => fa.onComplete(ta => k(ta.toEither)))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def insertAndDelete(step: InsertDeleteStep[PreInsertContract])(
      implicit log: doobie.LogHandler): ConnectionIO[Unit] = {
    import doobie.implicits._, cats.syntax.functor._
    surrogateTemplateIds(step.inserts.iterator.map(_.templateId).toSet).flatMap { stidMap =>
      import cats.syntax.apply._, cats.instances.vector._, scalaz.std.set._
      import json.JsonProtocol._
      import doobie.postgres.implicits._
      (Queries.deleteContracts(step.deletes) *>
        Queries.insertContracts(step.inserts map (dbc =>
          dbc copy (templateId = stidMap getOrElse (dbc.templateId, throw new IllegalStateException(
            "template ID missing from prior retrieval; impossible")),
          witnessParties = domain.Party.unsubst(dbc.witnessParties)))))
    }.void
  }

  final case class InsertDeleteStep[+C](inserts: Vector[C], deletes: Set[String]) {
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    def append[CC >: C](o: InsertDeleteStep[CC])(
        implicit cid: CC <~< DBContract[Any, Any, Any]): InsertDeleteStep[CC] =
      appendWithCid(o)(
        Liskov.contra1_2[Function1, DBContract[Any, Any, Any], CC, String](cid)(_.contractId))

    def appendWithCid[CC >: C](o: InsertDeleteStep[CC])(cid: CC => String): InsertDeleteStep[CC] =
      InsertDeleteStep(
        (if (o.deletes.isEmpty) inserts
         else inserts.filter(c => !o.deletes.contains(cid(c)))) ++ o.inserts,
        deletes union o.deletes)
  }

  private def transactionFilter(
      party: domain.Party,
      templateIds: List[TemplateId.RequiredPkg]): lav1.transaction_filter.TransactionFilter = {
    import lav1.transaction_filter._

    val filters =
      if (templateIds.isEmpty) Filters.defaultInstance
      else Filters(Some(lav1.transaction_filter.InclusiveFilters(templateIds.map(apiIdentifier))))

    TransactionFilter(Map(domain.Party.unwrap(party) -> filters))
  }
}
