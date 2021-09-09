// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.scalautil.Statement.discard
import com.daml.http.dbbackend.{ContractDao, SupportedJdbcDriver}
import com.daml.http.dbbackend.Queries.{DBContract, SurrogateTpId}
import com.daml.http.domain.TemplateId
import com.daml.http.LedgerClientJwt.Terminates
import com.daml.http.util.ApiValueToLfValueConverter.apiValueToLfValue
import com.daml.http.json.JsonProtocol.LfValueDatabaseCodec.{
  apiValueToJsValue => lfValueToDbJsValue
}
import com.daml.http.util.IdentifierConverters.apiIdentifier
import com.daml.http.util.Logging.{InstanceUUID}
import util.{AbsoluteBookmark, BeginBookmark, ContractStreamStep, InsertDeleteStep, LedgerBegin}
import com.daml.scalautil.ExceptionOps._
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.{v1 => lav1}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import doobie.free.{connection => fconn}
import fconn.ConnectionIO
import scalaz.Order
import scalaz.OneAnd._
import scalaz.std.set._
import scalaz.std.vector._
import scalaz.std.list._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.functor._
import scalaz.syntax.foldable._
import scalaz.syntax.order._
import scalaz.syntax.std.option._
import scalaz.{-\/, OneAnd, \/, \/-}
import spray.json.{JsNull, JsValue}
import scalaz.Liskov.<~<

import scala.concurrent.{ExecutionContext, Future}
import com.daml.ledger.api.{domain => LedgerApiDomain}

private class ContractsFetch(
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    getTermination: LedgerClientJwt.GetTermination,
)(implicit dblog: doobie.LogHandler, sjd: SupportedJdbcDriver.TC) {

  import ContractsFetch._
  import sjd.retrySqlStates

  private[this] val logger = ContextualizedLogger.get(getClass)

  def fetchAndPersist(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: OneAnd[Set, domain.Party],
      templateIds: List[domain.TemplateId.RequiredPkg],
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[BeginBookmark[Terminates.AtAbsolute]] = {
    import cats.instances.list._, cats.syntax.foldable.{toFoldableOps => ToFoldableOps},
    cats.syntax.traverse.{toTraverseOps => ToTraverseOps}, cats.syntax.functor._, doobie.implicits._
    // we can fetch for all templateIds on a single acsFollowingAndBoundary
    // by comparing begin offsets; however this is trickier so we don't do it
    // right now -- Stephen / Leo
    connectionIOFuture(getTermination(jwt, ledgerId)) flatMap {
      _.cata(
        absEnd =>
          // traverse once, use the max _returned_ bookmark,
          // re-traverse any that != the max returned bookmark (overriding lastOffset)
          // fetch cannot go "too far" the second time
          templateIds.traverse(fetchAndPersist(jwt, ledgerId, parties, false, absEnd, _)).flatMap {
            actualAbsEnds =>
              val newAbsEndTarget = {
                import scalaz.std.list._, scalaz.syntax.foldable._,
                domain.Offset.{ordering => `Offset ordering`}
                // it's fine if all yielded LedgerBegin, so we don't want to conflate the "fallback"
                // with genuine results
                actualAbsEnds.maximum getOrElse AbsoluteBookmark(absEnd.toDomain)
              }
              newAbsEndTarget match {
                case LedgerBegin =>
                  fconn.pure(AbsoluteBookmark(absEnd))
                case AbsoluteBookmark(feedback) =>
                  val feedbackTerminator =
                    Terminates
                      .AtAbsolute(lav1.ledger_offset.LedgerOffset.Value.Absolute(feedback.unwrap))
                  // contractsFromOffsetIo can go _past_ absEnd, because the ACS ignores
                  // this argument; see https://github.com/digital-asset/daml/pull/8226#issuecomment-756446537
                  // for an example of this happening.  We deal with this race condition
                  // by detecting that it has happened and rerunning any other template IDs
                  // to "catch them up" to the one that "raced" ahead
                  (actualAbsEnds zip templateIds)
                    .collect { case (`newAbsEndTarget`, templateId) => templateId }
                    .traverse_ {
                      // passing a priorBookmark prevents contractsIo_ from using the ACS,
                      // and it cannot go "too far" reading only the tx stream
                      fetchAndPersist(
                        jwt,
                        ledgerId,
                        parties,
                        true,
                        feedbackTerminator,
                        _,
                      )
                    }
                    .as(AbsoluteBookmark(feedbackTerminator))
              }
          },
        fconn.pure(LedgerBegin),
      )
    }

  }

  private[this] def fetchAndPersist(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: OneAnd[Set, domain.Party],
      disableAcs: Boolean,
      absEnd: Terminates.AtAbsolute,
      templateId: domain.TemplateId.RequiredPkg,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[BeginBookmark[domain.Offset]] = {

    import doobie.implicits._, cats.syntax.apply._

    def loop(maxAttempts: Int): ConnectionIO[BeginBookmark[domain.Offset]] = {
      logger.debug(s"contractsIo, maxAttempts: $maxAttempts")
      (contractsIo_(jwt, ledgerId, parties, disableAcs, absEnd, templateId) <* fconn.commit)
        .exceptSql {
          case e if maxAttempts > 0 && retrySqlStates(e.getSQLState) =>
            logger.debug(s"contractsIo, exception: ${e.description}, state: ${e.getSQLState}")
            fconn.rollback flatMap (_ => loop(maxAttempts - 1))
          case e @ _ =>
            logger.error(s"contractsIo3 exception: ${e.description}, state: ${e.getSQLState}")
            fconn.raiseError(e)
        }
    }

    loop(5)
  }

  private def contractsIo_(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: OneAnd[Set, domain.Party],
      disableAcs: Boolean,
      absEnd: Terminates.AtAbsolute,
      templateId: domain.TemplateId.RequiredPkg,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      lc: LoggingContextOf[InstanceUUID],
  ): ConnectionIO[BeginBookmark[domain.Offset]] =
    for {
      offsets <- ContractDao.lastOffset(parties, templateId)
      offset1 <- contractsFromOffsetIo(
        jwt,
        ledgerId,
        parties,
        templateId,
        offsets,
        disableAcs,
        absEnd,
      )
      _ = logger.debug(s"contractsFromOffsetIo($jwt, $parties, $templateId, $offsets): $offset1")
    } yield offset1

  private def prepareCreatedEventStorage(
      ce: lav1.event.CreatedEvent
  ): Exception \/ PreInsertContract = {
    import scalaz.syntax.traverse._
    import scalaz.std.option._
    import com.daml.lf.crypto.Hash
    for {
      ac <- domain.ActiveContract fromLedgerApi ce leftMap (de =>
        new IllegalArgumentException(s"contract ${ce.contractId}: ${de.shows}"): Exception,
      )
      lfKey <- ac.key.traverse(apiValueToLfValue).leftMap(_.cause: Exception)
      lfArg <- apiValueToLfValue(ac.payload) leftMap (_.cause: Exception)
    } yield DBContract(
      contractId = ac.contractId.unwrap,
      templateId = ac.templateId,
      key = lfKey.cata(lfValueToDbJsValue, JsNull),
      keyHash = lfKey.map(
        Hash
          .assertHashContractKey(TemplateId.toLedgerApiValue(ac.templateId), _)
          .toHexString
      ),
      payload = lfValueToDbJsValue(lfArg),
      signatories = ac.signatories,
      observers = ac.observers,
      agreementText = ac.agreementText,
    )
  }

  private def jsonifyInsertDeleteStep(
      a: InsertDeleteStep[Any, lav1.event.CreatedEvent]
  ): InsertDeleteStep[Unit, PreInsertContract] =
    a.leftMap(_ => ())
      .mapPreservingIds(prepareCreatedEventStorage(_) valueOr (e => throw e))

  private def contractsFromOffsetIo(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: OneAnd[Set, domain.Party],
      templateId: domain.TemplateId.RequiredPkg,
      offsets: Map[domain.Party, domain.Offset],
      disableAcs: Boolean,
      absEnd: Terminates.AtAbsolute,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): ConnectionIO[BeginBookmark[domain.Offset]] = {

    import domain.Offset._
    val offset = offsets.values.toList.minimum.cata(AbsoluteBookmark(_), LedgerBegin)

    val graph = RunnableGraph.fromGraph(
      GraphDSL.create(
        Sink.queue[ConnectionIO[Unit]](),
        Sink.last[BeginBookmark[String]],
      )(Keep.both) { implicit builder => (acsSink, offsetSink) =>
        import GraphDSL.Implicits._

        val txnK = getCreatesAndArchivesSince(
          jwt,
          ledgerId,
          transactionFilter(parties, List(templateId)),
          _: lav1.ledger_offset.LedgerOffset,
          absEnd,
        )

        // include ACS iff starting at LedgerBegin
        val (idses, lastOff) = (offset, disableAcs) match {
          case (LedgerBegin, false) =>
            val stepsAndOffset = builder add acsFollowingAndBoundary(txnK)
            stepsAndOffset.in <~ getActiveContracts(
              jwt,
              ledgerId,
              transactionFilter(parties, List(templateId)),
              true,
            )
            (stepsAndOffset.out0, stepsAndOffset.out1)

          case (AbsoluteBookmark(_), _) | (LedgerBegin, true) =>
            val stepsAndOffset = builder add transactionsFollowingBoundary(txnK)
            stepsAndOffset.in <~ Source.single(domain.Offset.tag.unsubst(offset))
            (
              (stepsAndOffset: FanOutShape2[_, ContractStreamStep.LAV1, _]).out0,
              stepsAndOffset.out1,
            )
        }

        val transactInsertsDeletes = Flow
          .fromFunction(jsonifyInsertDeleteStep)
          .conflate(_ append _)
          .map(insertAndDelete)

        idses.map(_.toInsertDelete) ~> transactInsertsDeletes ~> acsSink
        lastOff ~> offsetSink

        ClosedShape
      }
    )

    val (acsQueue, lastOffsetFuture) = graph.run()

    for {
      _ <- sinkCioSequence_(acsQueue)
      offset0 <- connectionIOFuture(lastOffsetFuture)
      offsetOrError <- offset0 match {
        case AbsoluteBookmark(str) =>
          val newOffset = domain.Offset(str)
          ContractDao
            .updateOffset(parties, templateId, newOffset, offsets)
            .map(_ => AbsoluteBookmark(newOffset))
        case LedgerBegin =>
          fconn.pure(LedgerBegin)
      }
    } yield offsetOrError
  }
}

private[http] object ContractsFetch {

  type PreInsertContract = DBContract[TemplateId.RequiredPkg, JsValue, JsValue, Seq[domain.Party]]

  def partition[A, B]: Graph[FanOutShape2[A \/ B, A, B], NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val split = b.add(
        Partition[A \/ B](
          2,
          {
            case -\/(_) => 0
            case \/-(_) => 1
          },
        )
      )
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

  private def max[A: Order](ifEmpty: A): Flow[A, A, NotUsed] =
    Flow[A].fold(ifEmpty)(_ max _)

  /** Plan inserts, deletes from an in-order batch of create/archive events. */
  private def partitionInsertsDeletes(
      txes: Iterable[lav1.event.Event]
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
        private val g: Graph[FanOutShape2[A, Y, Z], M]
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
      def divertToHead(implicit noM: M <~< NotUsed): Flow[A, Y, Future[Z]] = {
        type CK[-T] = (T, Future[Z]) => Future[Z]
        divertToMat(Sink.head)(noM.subst[CK](Keep.right[NotUsed, Future[Z]]))
      }
    }
  }

  /** Like `acsAndBoundary`, but also include the events produced by `transactionsSince`
    * after the ACS's last offset, terminating with the last offset of the last transaction,
    * or the ACS's last offset if there were no transactions.
    */
  private[http] def acsFollowingAndBoundary(
      transactionsSince: lav1.ledger_offset.LedgerOffset => Source[Transaction, NotUsed]
  ): Graph[FanOutShape2[
    lav1.active_contracts_service.GetActiveContractsResponse,
    ContractStreamStep.LAV1,
    BeginBookmark[String],
  ], NotUsed] =
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
      transactionsSince: lav1.ledger_offset.LedgerOffset => Source[Transaction, NotUsed]
  ): Graph[FanOutShape2[
    BeginBookmark[String],
    ContractStreamStep.Txn.LAV1,
    BeginBookmark[String],
  ], NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      type Off = BeginBookmark[String]
      val dupOff = b add Broadcast[Off](2)
      val mergeOff = b add Concat[Off](2)
      val txns = Flow[Off]
        .flatMapConcat(off => transactionsSince(domain.Offset.tag.subst(off).toLedgerApi))
        .map(transactionToInsertsAndDeletes)
      val txnSplit = b add project2[ContractStreamStep.Txn.LAV1, domain.Offset]
      import domain.Offset.{ordering => `Offset ordering`}
      val lastTxOff = b add last(LedgerBegin: Off)
      type EndoBookmarkFlow[A] = Flow[BeginBookmark[A], BeginBookmark[A], NotUsed]
      val maxOff = b add domain.Offset.tag.unsubst[EndoBookmarkFlow, String](
        max(LedgerBegin: BeginBookmark[domain.Offset])
      )
      // format: off
      discard { txnSplit.in <~ txns <~ dupOff }
      discard {                        dupOff                                       ~> mergeOff ~> maxOff }
      discard { txnSplit.out1.map(off => AbsoluteBookmark(off.unwrap)) ~> lastTxOff ~> mergeOff }
      // format: on
      new FanOutShape2(dupOff.in, txnSplit.out0, maxOff.out)
    }

  /** Split a series of ACS responses into two channels: one with contracts, the
    * other with a single result, the last offset.
    */
  private[this] def acsAndBoundary
      : Graph[FanOutShape2[lav1.active_contracts_service.GetActiveContractsResponse, Seq[
        lav1.event.CreatedEvent,
      ], BeginBookmark[String]], NotUsed] =
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
      tx: lav1.transaction.Transaction
  ): (ContractStreamStep.Txn.LAV1, domain.Offset) = {
    val offset = domain.Offset.fromLedgerApi(tx)
    (ContractStreamStep.Txn(partitionInsertsDeletes(tx.events), offset), offset)
  }

  private def surrogateTemplateIds[K <: TemplateId.RequiredPkg](
      ids: Set[K]
  )(implicit
      log: doobie.LogHandler,
      sjd: SupportedJdbcDriver.TC,
  ): ConnectionIO[Map[K, SurrogateTpId]] = {
    import doobie.implicits._, cats.instances.vector._, cats.syntax.functor._,
    cats.syntax.traverse._
    import sjd.q.queries.surrogateTemplateId
    ids.toVector
      .traverse(k => surrogateTemplateId(k.packageId, k.moduleName, k.entityName) tupleLeft k)
      .map(_.toMap)
  }

  private def sinkCioSequence_[Ign](
      f: SinkQueueWithCancel[doobie.ConnectionIO[Ign]]
  )(implicit ec: ExecutionContext): doobie.ConnectionIO[Unit] = {
    import doobie.ConnectionIO
    import doobie.free.{connection => fconn}
    def go(): ConnectionIO[Unit] = {
      val next = f.pull()
      connectionIOFuture(next)
        .flatMap(_.cata(cio => cio flatMap (_ => go()), fconn.pure(())))
    }
    fconn.handleErrorWith(
      go(),
      t => {
        f.cancel()
        fconn.raiseError(t)
      },
    )
  }

  private def connectionIOFuture[A](
      fa: Future[A]
  )(implicit ec: ExecutionContext): doobie.ConnectionIO[A] =
    fconn.async[A](k => fa.onComplete(ta => k(ta.toEither)))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def insertAndDelete(
      step: InsertDeleteStep[Any, PreInsertContract]
  )(implicit log: doobie.LogHandler, sjd: SupportedJdbcDriver.TC): ConnectionIO[Unit] = {
    import doobie.implicits._, cats.syntax.functor._
    surrogateTemplateIds(step.inserts.iterator.map(_.templateId).toSet).flatMap { stidMap =>
      import cats.syntax.apply._, cats.instances.vector._
      import json.JsonProtocol._
      import sjd.q.queries
      (queries.deleteContracts(step.deletes.keySet) *>
        queries.insertContracts(
          step.inserts map (dbc =>
            dbc.copy(
              templateId = stidMap.getOrElse(
                dbc.templateId,
                throw new IllegalStateException(
                  "template ID missing from prior retrieval; impossible"
                ),
              ),
              signatories = domain.Party.unsubst(dbc.signatories),
              observers = domain.Party.unsubst(dbc.observers),
            ),
          )
        ))
    }.void
  }

  private def transactionFilter(
      parties: OneAnd[Set, domain.Party],
      templateIds: List[TemplateId.RequiredPkg],
  ): lav1.transaction_filter.TransactionFilter = {
    import lav1.transaction_filter._

    val filters =
      if (templateIds.isEmpty) Filters.defaultInstance
      else Filters(Some(lav1.transaction_filter.InclusiveFilters(templateIds.map(apiIdentifier))))

    TransactionFilter(domain.Party.unsubst(parties.toVector).map(_ -> filters).toMap)
  }
}
