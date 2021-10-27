// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts

import akka.NotUsed
import akka.stream.scaladsl.{Broadcast, Concat, Flow, GraphDSL, Source}
import akka.stream.{FanOutShape2, Graph}
import com.daml.scalautil.Statement.discard
import domain.TemplateId
import util.{AbsoluteBookmark, BeginBookmark, ContractStreamStep, InsertDeleteStep, LedgerBegin}
import util.IdentifierConverters.apiIdentifier
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.{v1 => lav1}
import scalaz.syntax.tag._

private[daml] object AcsTxStreams {
  import util.AkkaStreamsDoobie.{last, max, project2}

  /** Plan inserts, deletes from an in-order batch of create/archive events. */
  private[this] def partitionInsertsDeletes(
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

  /** Like `acsAndBoundary`, but also include the events produced by `transactionsSince`
    * after the ACS's last offset, terminating with the last offset of the last transaction,
    * or the ACS's last offset if there were no transactions.
    */
  private[daml] def acsFollowingAndBoundary(
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

  /** Interpreting the transaction stream so it conveniently depends on
    * the ACS graph, if desired.  Deliberately matching output shape
    * to `acsFollowingAndBoundary`.
    */
  private[daml] def transactionsFollowingBoundary(
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
      import domain.Offset.`Offset ordering`
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

  private[this] def transactionToInsertsAndDeletes(
      tx: lav1.transaction.Transaction
  ): (ContractStreamStep.Txn.LAV1, domain.Offset) = {
    val offset = domain.Offset.fromLedgerApi(tx)
    (ContractStreamStep.Txn(partitionInsertsDeletes(tx.events), offset), offset)
  }

  private[daml] def transactionFilter(
      parties: domain.PartySet,
      templateIds: List[TemplateId.RequiredPkg],
  ): lav1.transaction_filter.TransactionFilter = {
    import lav1.transaction_filter._

    val filters =
      if (templateIds.isEmpty) Filters.defaultInstance
      else Filters(Some(lav1.transaction_filter.InclusiveFilters(templateIds.map(apiIdentifier))))

    TransactionFilter(
      domain.Party.unsubst((parties: Set[domain.Party]).toVector).map(_ -> filters).toMap
    )
  }
}
