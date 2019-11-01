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
import cats.effect.IO
import com.digitalasset.http.Statement.discard
import com.digitalasset.http.dbbackend.{ContractDao, Queries}
import com.digitalasset.http.dbbackend.Queries.{DBContract, SurrogateTpId}
import com.digitalasset.http.domain.TemplateId
import com.digitalasset.http.util.ApiValueToLfValueConverter.apiValueToLfValue
import com.digitalasset.http.json.JsonProtocol.LfValueDatabaseCodec.{
  apiValueToJsValue => lfValueToDbJsValue
}
import com.digitalasset.http.util.IdentifierConverters.apiIdentifier
import com.digitalasset.jwt.domain.Jwt
import com.digitalasset.ledger.api.{v1 => lav1}

import doobie.free.connection
import doobie.free.connection.ConnectionIO
import scalaz.std.vector._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.functor._
import scalaz.syntax.std.option._
import scalaz.{-\/, \/, \/-}
import spray.json.JsValue

import scala.concurrent.{ExecutionContext, Future}

private class ContractsFetch(
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    lookupType: query.ValuePredicate.TypeLookup,
    contractDao: Option[dbbackend.ContractDao],
)(implicit dblog: doobie.LogHandler) {

  import ContractsFetch._

  def contracts(jwt: Jwt, party: domain.Party, templateId: domain.TemplateId.RequiredPkg)(
      implicit ec: ExecutionContext,
      mat: Materializer): ConnectionIO[domain.Error \/ Unit] = {

    val statement: ConnectionIO[Unit] = for {
      offset <- readOffsetFromDbOrFetchFromLedgerAndUpdateDb(jwt, party, templateId)
    } yield ???

    ???

  }

  private def readOffsetFromDbOrFetchFromLedgerAndUpdateDb(
      jwt: Jwt,
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg)(
      implicit ec: ExecutionContext,
      mat: Materializer): ConnectionIO[domain.Error \/ domain.Offset] =
    for {
      offsetO <- readLastOffsetFromDb(party, templateId): ConnectionIO[Option[domain.Offset]]
      offsetE <- offsetO.cata(
        x => doobie.free.connection.pure(\/-(x)),
        contractsToOffsetIo(jwt, party, templateId)
      ): ConnectionIO[domain.Error \/ domain.Offset]
    } yield offsetE

  private def contractsToOffsetIo(
      jwt: Jwt,
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg
  )(
      implicit ec: ExecutionContext,
      mat: Materializer): ConnectionIO[domain.Error \/ domain.Offset] = {

    val graph = RunnableGraph.fromGraph(
      GraphDSL.create(
        Sink.queue[ConnectionIO[Unit]](),
        Sink.last[lav1.ledger_offset.LedgerOffset]
      )((a, b) => (a, b)) { implicit builder => (acsSink, offsetSink) =>
        import GraphDSL.Implicits._

        val initialAcsSource =
          getActiveContracts(jwt, transactionFilter(party, List(templateId)), true)
        val acsAndOffset = builder add acsAndBoundary

        acsAndOffset.in <~ initialAcsSource
        acsAndOffset.out0 ~> jsonifyCreates ~> persistInitialActiveContracts ~> acsSink
        acsAndOffset.out1 ~> offsetSink

        ClosedShape
      })

    val (acsQueue, lastOffsetFuture) = graph.run()

    import lav1.ledger_offset.LedgerOffset, LedgerOffset.Value.Absolute
    for {
      _ <- sinkCioSequence_(acsQueue)
      ledgerOffset <- connectionIOFuture(lastOffsetFuture)
      offsetOrError <- ledgerOffset match {
        case LedgerOffset(Absolute(str)) =>
          val offset = domain.Offset(str)
          ContractDao.updateOffset(party, templateId, offset).map(_ => \/-(offset))
        case x @ _ =>
          doobie.free.connection.pure(
            -\/(
              domain
                .Error('contractsToOffsetIo, s"expected LedgerOffset(Absolute(String)), got: $x")))
      }
    } yield offsetOrError
  }

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
  private def jsonifyCreates
    : Flow[Seq[lav1.event.CreatedEvent], Vector[PreInsertContract], NotUsed] = {
    import scalaz.syntax.traverse._
    Flow fromFunction (_.toVector traverse prepareCreatedEventStorage fold (e => throw e, identity))
  }

  private def persistInitialActiveContracts
    : Flow[Vector[PreInsertContract], ConnectionIO[Unit], NotUsed] =
    Flow[Vector[PreInsertContract]]
      .map(planAcsBlockInserts)
      .conflate(_.append(_)(_.contractId))
      .map(insertAndDelete)

  private def readLastOffsetFromDb(
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg): ConnectionIO[Option[domain.Offset]] =
    contractDao match {
      case Some(dao) => ContractDao.lastOffset(party, templateId)
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

  type PreInsertContract = DBContract[TemplateId.RequiredPkg, JsValue, Seq[domain.Party]]

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
      gacrs: Traversable[PreInsertContract]): InsertDeleteStep[PreInsertContract] =
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
      (Queries.deleteContracts(step.deletes) *>
        Queries.insertContracts(step.inserts map (dbc =>
          dbc copy (templateId = stidMap getOrElse (dbc.templateId, throw new IllegalStateException(
            "template ID missing from prior retrieval; impossible"))))))
    }.void
  }

  final case class InsertDeleteStep[+C](inserts: Vector[C], deletes: Set[String]) {
    def append[CC >: C](o: InsertDeleteStep[CC])(cid: CC => String): InsertDeleteStep[CC] =
      InsertDeleteStep(
        (if (o.deletes.isEmpty) inserts
         else inserts.filter(c => !o.deletes.contains(cid(c)))) ++ o.inserts,
        deletes union o.deletes)
  }

}
