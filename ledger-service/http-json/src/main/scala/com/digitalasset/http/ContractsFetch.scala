// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.stream.scaladsl.{
  Broadcast,
  Flow,
  GraphDSL,
  Partition,
  RunnableGraph,
  Sink,
  SinkQueueWithCancel,
  Source
}
import akka.stream.{ClosedShape, FanOutShape2, Graph, Materializer}
import cats.effect.{ContextShift, IO}
import com.digitalasset.http.ContractsService.ActiveContract
import com.digitalasset.http.Statement.discard
import com.digitalasset.http.domain.TemplateId
import com.digitalasset.http.util.IdentifierConverters.apiIdentifier
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.{v1 => lav1}
import doobie.free.connection
import doobie.free.connection.ConnectionIO
import scalaz.std.tuple._
import scalaz.syntax.functor._
import scalaz.syntax.std.option._
import scalaz.{-\/, \/, \/-}

import scala.concurrent.{ExecutionContext, Future}

private class ContractsFetch(
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    lookupType: query.ValuePredicate.TypeLookup,
    contractDao: Option[dbbackend.ContractDao],
) {

  import ContractsFetch._

  def fetchActiveContractsFromOffset(
      dao: Option[dbbackend.ContractDao],
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg)(implicit cs: ContextShift[IO]) = {

    val statement: ConnectionIO[Unit] = for {
      offset <- readLastOffsetFromDb(party, templateId)

    } yield ???

    dao.map(x => x.transact(statement)) match {
      case Some(x) => x.map(Some(_))
      case None => IO.pure(None)
    }
  }

  private def contractsToOffsetIo(
      jwt: Jwt,
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg
  )(
      implicit ec: ExecutionContext,
      mat: Materializer): ConnectionIO[lav1.ledger_offset.LedgerOffset] = {

    val graph = RunnableGraph.fromGraph(
      GraphDSL.create(
        Sink.queue[Seq[lav1.event.CreatedEvent]](),
        Sink.last[lav1.ledger_offset.LedgerOffset]
      )((a, b) => (a, b)) { implicit builder => (acsSink, offsetSink) =>
        import GraphDSL.Implicits._

        val initialAcsSource =
          getActiveContracts(jwt, transactionFilter(party, List(templateId)), true)
        val acsAndOffset = builder add acsAndBoundary
//        val persistInitialAcs = builder add persistInitialActiveContracts

        // TODO add a stage to persist ACS
        // convert to DBContract (with proper JSON createargs), make InsertDeleteStep,
        // conflate ++, InsertDeleteStep => ConIO
        initialAcsSource ~> acsAndOffset.in
//        acsAndOffset.out0 ~> persistInitialAcs ~> acsSink
        acsAndOffset.out0 ~> acsSink
        acsAndOffset.out1 ~> offsetSink

        ClosedShape
      })

    val (acsQueue, lastOffsetFuture) = graph.run()

//    for {
//      _ <- sinkCioSequence_(acsQueue)
//      offset <- connectionIOFuture(lastOffsetFuture)
//    } yield offset

    ???
  }

//  private def persistInitialActiveContracts: Flow[Seq[lav1.event.CreatedEvent], ConnectionIO[Int], NotUsed] =
//    ???

  private def readLastOffsetFromDb(
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg): ConnectionIO[Option[domain.Offset]] =
    contractDao match {
      case Some(dao) => dao.lastOffset(party, templateId)
      case None => connection.pure(None)
    }

  private val initialActiveContracts: Flow[
    lav1.active_contracts_service.GetActiveContractsResponse,
    domain.Error \/ (Contract, Option[domain.Offset]),
    NotUsed
  ] =
    Flow[lav1.active_contracts_service.GetActiveContractsResponse].mapConcat { gacr =>
      val offset = domain.Offset.fromLedgerApi(gacr)
      unsequence(offset)(domain.Contract.fromLedgerApi(gacr))
    }

  private val transactionContracts: Flow[
    lav1.transaction.Transaction,
    domain.Error \/ (Contract, domain.Offset),
    NotUsed
  ] =
    Flow[lav1.transaction.Transaction]
      .mapConcat { tx =>
        val offset = domain.Offset.fromLedgerApi(tx)
        unsequence(offset)(domain.Contract.fromLedgerApi(tx))
      }

  private def unsequence[A, B, C](c: C)(fbs: A \/ List[B]): List[A \/ (B, C)] =
    fbs.fold(
      a => List(-\/(a)),
      bs => bs.map(b => \/-((b, c)))
    )

  private def ioToSource[Out](io: IO[Out]): Source[Out, NotUsed] =
    Source.fromFuture(io.unsafeToFuture())

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

private object ContractsFetch {
  type Contract = domain.Contract[lav1.value.Value]

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

  /** Plan inserts from an ACS response. */
  private def planAcsBlockInserts(
      gacrs: Traversable[lav1.event.CreatedEvent]): InsertDeleteStep[lav1.event.CreatedEvent] =
    InsertDeleteStep(gacrs.toVector, Set.empty)

  /** Plan inserts, deletes from an in-order batch of create/archive events. */
  /*TODO SC private*/
  def partitionInsertsDeletes(
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

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  /*TODO SC private*/
  def contractsToOffset(implicit ec: ExecutionContext): Sink[
    lav1.active_contracts_service.GetActiveContractsResponse,
    Future[(Seq[ActiveContract], lav1.ledger_offset.LedgerOffset)]] = {
    import lav1.ledger_offset.LedgerOffset
    import LedgerOffset.{LedgerBoundary, Value}
    import Value.{Absolute, Boundary}
    Sink
      .fold[
        (Vector[ActiveContract], Value),
        lav1.active_contracts_service.GetActiveContractsResponse](
        (Vector.empty, Boundary(LedgerBoundary.LEDGER_BEGIN))) { (s, gacr) =>
        (s._1 ++ gacr.activeContracts.map(_ => ???), Absolute(gacr.offset))
      }
      .mapMaterializedValue(_ map (_ map LedgerOffset.apply))
  }

  /** Split a series of ACS responses into two channels: one with contracts, the
    * other with a single result, the last offset.
    */
  private def acsAndBoundary: Graph[
    FanOutShape2[
      lav1.active_contracts_service.GetActiveContractsResponse,
      Seq[lav1.event.CreatedEvent],
      lav1.ledger_offset.LedgerOffset],
    NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      import lav1.active_contracts_service.{GetActiveContractsResponse => GACR}
      import lav1.ledger_offset.LedgerOffset
      import LedgerOffset.{LedgerBoundary, Value}
      import Value.{Absolute, Boundary}
      val dup = b add Broadcast[GACR](2)
      val acs = b add (Flow fromFunction ((_: GACR).activeContracts))
      val off = b add Flow[GACR]
        .collect { case gacr if gacr.offset.nonEmpty => Absolute(gacr.offset) }
        .fold(Boundary(LedgerBoundary.LEDGER_BEGIN): Value)((_, later) => later)
        .map(LedgerOffset.apply)
      discard { dup ~> acs }
      discard { dup ~> off }
      new FanOutShape2(dup.in, acs.out, off.out)
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

  final case class InsertDeleteStep[+C](inserts: Vector[C], deletes: Set[String]) {
    def append[CC >: C](o: InsertDeleteStep[CC])(cid: CC => String): InsertDeleteStep[CC] =
      InsertDeleteStep(
        (if (o.deletes.isEmpty) inserts
         else inserts.filter(c => !o.deletes.contains(cid(c)))) ++ o.inserts,
        deletes union o.deletes)
  }

}
