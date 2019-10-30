// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.NotUsed
import akka.stream.scaladsl.{
  Broadcast,
  Flow,
  GraphDSL,
  Partition,
  Sink,
  SinkQueueWithCancel,
  Source
}
import akka.stream.{FanOutShape2, Graph}
import cats.effect.{ContextShift, IO}
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.http.ContractsService.ActiveContract
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.v1.transaction_filter.TransactionFilter
import com.digitalasset.ledger.api.{v1 => lav1}
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
      templateId: domain.TemplateId.RequiredPkg)(implicit cs: ContextShift[IO]) =
    for {
      _ <- IO.shift(cs)
//      a <- dao
//      x = fetchActiveContractsFromOffset(???, party, templateId)
    } yield ???

  private def fetchInitialActiveContractSet(jwt: Jwt, txFilter: TransactionFilter)
    : Source[domain.Error \/ (Contract, Option[domain.Offset]), NotUsed] =
    getActiveContracts(jwt, txFilter, true).mapConcat { gacr =>
      val offset = domain.Offset.fromLedgerApi(gacr)
      unsequence(offset)(domain.Contract.fromLedgerApi(gacr))
    }

  private def fetchActiveContractsFromOffset(
      jwt: Jwt,
      txFilter: TransactionFilter,
      offset: domain.Offset): Source[domain.Error \/ (Contract, domain.Offset), NotUsed] =
    getCreatesAndArchivesSince(jwt, txFilter, domain.Offset.toLedgerApi(offset))
      .via(transactionContracts)

  private def transactionContracts
    : Flow[lav1.transaction.Transaction, domain.Error \/ (Contract, domain.Offset), NotUsed] =
    Flow[lav1.transaction.Transaction]
      .mapConcat { tx =>
        val offset = domain.Offset.fromLedgerApi(tx)
        unsequence(offset)(domain.Contract.fromLedgerApi(tx))
      }

  private def unsequence[A, B, C](c: C)(as: A \/ List[B]): List[A \/ (B, C)] =
    as.fold(
      a => List(-\/(a)),
      bs => bs.map(b => \/-((b, c)))
    )

  private def ioToSource[Out](io: IO[Out]): Source[Out, NotUsed] =
    Source.fromFuture(io.unsafeToFuture())
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
      split ~> as
      split ~> bs
      new FanOutShape2(split.in, as.out, bs.out)
    }

  /** Plan inserts, deletes from an in-order batch of create/archive events. */
  /*TODO SC private*/
  def partitionInsertsDeletes(
      txes: Traversable[lav1.event.Event]): InsertDeleteStep[lav1.event.CreatedEvent] = {
    val csb = ImmArraySeq.newBuilder[lav1.event.CreatedEvent]
    val asb = Set.newBuilder[String]
    import lav1.event.Event
    import Event.Event._
    txes foreach {
      case Event(Created(c)) => csb += c; ()
      case Event(Archived(a)) => asb += a.contractId; ()
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
    * other with one or more offsets, of which all but the last should be ignored.
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
        .collect { case gacr if gacr.offset.nonEmpty => Absolute(gacr.offset): Value }
        .prepend(Source single Boundary(LedgerBoundary.LEDGER_BEGIN))
        .conflate((_, later) => later)
        .map(LedgerOffset.apply)
      dup ~> acs
      dup ~> off
      new FanOutShape2(dup.in, acs.out, off.out)
    }

  private def sinkCioSequence_[Ign](f: SinkQueueWithCancel[doobie.ConnectionIO[Ign]])(
      implicit ec: ExecutionContext): doobie.ConnectionIO[Unit] = {
    import doobie.ConnectionIO, doobie.free.{connection => fconn}
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

  final case class InsertDeleteStep[+C](inserts: ImmArraySeq[C], deletes: Set[String]) {
    def append[CC >: C](o: InsertDeleteStep[CC])(cid: CC => String): InsertDeleteStep[CC] =
      InsertDeleteStep(
        inserts.filter(c => !o.deletes.contains(cid(c))) ++ o.inserts,
        deletes union o.deletes)
  }

}
