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
import doobie.free.connection
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
import com.digitalasset.ledger.api.v1.transaction.Transaction
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
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future}

private class ContractsFetch(
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    lookupType: query.ValuePredicate.TypeLookup
)(implicit dblog: doobie.LogHandler)
    extends StrictLogging {

  import ContractsFetch._

  def contractsIo2(jwt: Jwt, party: domain.Party, templateIds: List[domain.TemplateId.RequiredPkg])(
      implicit ec: ExecutionContext,
      mat: Materializer): ConnectionIO[List[domain.Offset]] = {
    // TODO(Leo/Stephen): can we run this traverse concurrently?
    cats.implicits.catsStdInstancesForList.traverse(templateIds) { templateId =>
      contractsIo(jwt, party, templateId)
    }(connection.AsyncConnectionIO)
  }

  def contractsIo(jwt: Jwt, party: domain.Party, templateId: domain.TemplateId.RequiredPkg)(
      implicit ec: ExecutionContext,
      mat: Materializer): ConnectionIO[domain.Offset] =
    for {
      offset0 <- readOffsetFromDbOrFetchFromLedger(jwt, party, templateId)
      _ = logger.debug(s"readOffsetFromDbOrFetchFromLedger($jwt, $party, $templateId): $offset0")
      offset1 <- contractsFromOffsetIo(jwt, party, templateId, offset0)
      _ = logger.debug(s"contractsFromOffsetIo($jwt, $party, $templateId, $offset0): $offset1")
    } yield offset1

  private def readOffsetFromDbOrFetchFromLedger(
      jwt: Jwt,
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg)(
      implicit ec: ExecutionContext,
      mat: Materializer): ConnectionIO[domain.Offset] =
    for {
      offsetO <- ContractDao.lastOffset(party, templateId): ConnectionIO[Option[domain.Offset]]
      offset <- offsetO.cata(
        x => connection.pure(x),
        contractsToOffsetIo(jwt, party, templateId)
      ): ConnectionIO[domain.Offset]
    } yield offset

  private def contractsToOffsetIo(
      jwt: Jwt,
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg
  )(implicit ec: ExecutionContext, mat: Materializer): ConnectionIO[domain.Offset] = {

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
          ContractDao.updateOffset(party, templateId, offset).map(_ => offset)
        case x @ _ =>
          val errorMsg = s"expected LedgerOffset(Absolute(String)), got: $x"
          connection.raiseError(new IllegalStateException(errorMsg))
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

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def jsonifyInsertDeleteStep(
      a: InsertDeleteStep[lav1.event.CreatedEvent]): InsertDeleteStep[PreInsertContract] = {
    import scalaz.syntax.traverse._
    InsertDeleteStep(
      a.inserts traverse prepareCreatedEventStorage fold (e => throw e, identity),
      a.deletes
    )
  }

  private def persistInitialActiveContracts
    : Flow[Vector[PreInsertContract], ConnectionIO[Unit], NotUsed] =
    Flow[Vector[PreInsertContract]]
      .map(planAcsBlockInserts)
      .conflate(_.append(_)(_.contractId))
      .map(insertAndDelete)

  private def aggregate(
      a: InsertDeleteStep[PreInsertContract],
      b: InsertDeleteStep[PreInsertContract]): InsertDeleteStep[PreInsertContract] = {
    def f(x: PreInsertContract): String = x.contractId
    a.append(b)(f)
  }

  private def contractsFromOffsetIo(
      jwt: Jwt,
      party: domain.Party,
      templateId: domain.TemplateId.RequiredPkg,
      offset: domain.Offset)(
      implicit ec: ExecutionContext,
      mat: Materializer): ConnectionIO[domain.Offset] = {

    val graph = RunnableGraph.fromGraph(
      GraphDSL.create(
        Sink.queue[ConnectionIO[Unit]](),
        Sink.last[domain.Offset]
      )((a, b) => (a, b)) { implicit builder => (acsSink, offsetSink) =>
        import GraphDSL.Implicits._

        val txSource: Source[Transaction, NotUsed] = getCreatesAndArchivesSince(
          jwt,
          transactionFilter(party, List(templateId)),
          domain.Offset.toLedgerApi(offset))

        val broadcast = builder add
          Broadcast[(InsertDeleteStep[lav1.event.CreatedEvent], domain.Offset)](2)

        def f(a: (InsertDeleteStep[lav1.event.CreatedEvent], domain.Offset))
          : InsertDeleteStep[PreInsertContract] = jsonifyInsertDeleteStep(a._1)

        txSource.map(transactionToInsertsAndDeletes) ~> broadcast.in
        broadcast.out(0).map(f).conflate(aggregate).map(insertAndDelete) ~> acsSink
        broadcast.out(1).map(_._2) ~> offsetSink

        ClosedShape
      })

    val (acsQueue, lastOffsetFuture) = graph.run()

    for {
      _ <- sinkCioSequence_(acsQueue)
      offset <- connectionIOFuture(lastOffsetFuture)
      _ <- ContractDao.updateOffset(party, templateId, offset)
    } yield offset
  }

  private def transactionToInsertsAndDeletes(tx: lav1.transaction.Transaction)
    : (InsertDeleteStep[lav1.event.CreatedEvent], domain.Offset) = {
    val offset = domain.Offset.fromLedgerApi(tx)
    (partitionInsertsDeletes(tx.events), offset)
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
