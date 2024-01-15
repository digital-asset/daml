// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.fetchcontracts

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Broadcast, Concat, Flow, GraphDSL, Source}
import org.apache.pekko.stream.{FanOutShape2, Graph}
import com.digitalasset.canton.fetchcontracts.util.GraphExtensions.*
import com.digitalasset.canton.fetchcontracts.util.IdentifierConverters.apiIdentifier
import com.daml.ledger.api.v1 as lav1
import com.daml.ledger.api.v2 as lav2
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.http.domain.{ContractTypeId, ResolvedQuery}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.NoTracing
import util.{AbsoluteBookmark, BeginBookmark, ContractStreamStep, InsertDeleteStep, LedgerBegin}

object AcsTxStreams extends NoTracing {
  import util.PekkoStreamsUtils.{last, max, project2}

  /** Plan inserts, deletes from an in-order batch of create/archive events. */
  private[this] def partitionInsertsDeletes(
      txes: Iterable[lav1.event.Event]
  ): InsertDeleteStep.LAV1 = {
    val csb = Vector.newBuilder[lav1.event.CreatedEvent]
    val asb = Map.newBuilder[String, lav1.event.ArchivedEvent]
    import lav1.event.Event
    import Event.Event.*
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
  def acsFollowingAndBoundary(
      transactionsSince: lav1.ledger_offset.LedgerOffset => Source[Transaction, NotUsed],
      logger: TracedLogger,
  )(implicit
      ec: concurrent.ExecutionContext,
      lc: com.daml.logging.LoggingContextOf[Any],
  ): Graph[FanOutShape2[
    lav2.state_service.GetActiveContractsResponse,
    ContractStreamStep.LAV1,
    BeginBookmark[domain.Offset],
  ], NotUsed] =
    GraphDSL.create() { implicit b =>
      import ContractStreamStep.{Acs, LiveBegin}
      import GraphDSL.Implicits.*
      type Off = BeginBookmark[domain.Offset]
      val acs = b add acsAndBoundary
      val dupOff = b add Broadcast[Off](2, eagerCancel = false)
      val liveStart = Flow fromFunction { (off: Off) =>
        LiveBegin(off)
      }
      val txns = b add transactionsFollowingBoundary(transactionsSince, logger)
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
      : Graph[FanOutShape2[lav2.state_service.GetActiveContractsResponse, Seq[
        lav1.event.CreatedEvent,
      ], BeginBookmark[domain.Offset]], NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits.*
      import lav2.state_service.GetActiveContractsResponse as GACR
      val dup = b add Broadcast[GACR](2, eagerCancel = true)
      val acs = b add (Flow fromFunction ((_: GACR).contractEntry.activeContract
        .flatMap(_.createdEvent)
        .toSeq))
      val off = b add Flow[GACR]
        .collect {
          case gacr if gacr.offset.nonEmpty => AbsoluteBookmark(domain.Offset(gacr.offset))
        }
        .via(last(LedgerBegin: BeginBookmark[domain.Offset]))
      discard { dup ~> acs }
      discard { dup ~> off }
      new FanOutShape2(dup.in, acs.out, off.out)
    }

  /** Interpreting the transaction stream so it conveniently depends on
    * the ACS graph, if desired.  Deliberately matching output shape
    * to `acsFollowingAndBoundary`.
    */
  def transactionsFollowingBoundary(
      transactionsSince: lav1.ledger_offset.LedgerOffset => Source[Transaction, NotUsed],
      logger: TracedLogger,
  )(implicit
      ec: concurrent.ExecutionContext,
      lc: com.daml.logging.LoggingContextOf[Any],
  ): Graph[FanOutShape2[
    BeginBookmark[domain.Offset],
    ContractStreamStep.Txn.LAV1,
    BeginBookmark[domain.Offset],
  ], NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits.*
      type Off = BeginBookmark[domain.Offset]
      val dupOff = b add Broadcast[Off](2)
      val mergeOff = b add Concat[Off](2)
      val txns = Flow[Off]
        .flatMapConcat(off => transactionsSince(off.toLedgerApi))
        .map(transactionToInsertsAndDeletes)
      val txnSplit = b add project2[ContractStreamStep.Txn.LAV1, domain.Offset]
      import domain.Offset.`Offset ordering`
      val lastTxOff = b add last(LedgerBegin: Off)
      val maxOff = b add max(LedgerBegin: Off)
      val logTxnOut =
        b add logTermination[ContractStreamStep.Txn.LAV1](logger, "first branch of tx stream split")
      // format: off
      discard { txnSplit.in <~ txns <~ dupOff }
      discard {                        dupOff                                ~> mergeOff ~> maxOff }
      discard { txnSplit.out1.map(off => AbsoluteBookmark(off)) ~> lastTxOff ~> mergeOff }
      discard { txnSplit.out0 ~> logTxnOut }
      // format: on
      new FanOutShape2(dupOff.in, logTxnOut.out, maxOff.out)
    }

  private[this] def transactionToInsertsAndDeletes(
      tx: lav1.transaction.Transaction
  ): (ContractStreamStep.Txn.LAV1, domain.Offset) = {
    val offset = domain.Offset.fromLedgerApi(tx)
    (ContractStreamStep.Txn(partitionInsertsDeletes(tx.events), offset), offset)
  }

  // TODO(#15278) remove
  def transactionFilterV1(
      parties: domain.PartySet,
      contractTypeIds: List[ContractTypeId.Resolved],
  ): lav1.transaction_filter.TransactionFilter = {
    import lav1.transaction_filter.*

    val (templateIds, interfaceIds) = ResolvedQuery.partition(contractTypeIds)
    val filters = Filters(
      Some(
        lav1.transaction_filter.InclusiveFilters(
          templateIds = templateIds.map(apiIdentifier),
          interfaceFilters = interfaceIds.map(interfaceId =>
            InterfaceFilter(
              interfaceId = Some(apiIdentifier(interfaceId)),
              includeInterfaceView = true,
            )
          ),
        )
      )
    )

    TransactionFilter(
      domain.Party.unsubst((parties: Set[domain.Party]).toVector).map(_ -> filters).toMap
    )
  }

  def transactionFilter(
      parties: domain.PartySet,
      contractTypeIds: List[ContractTypeId.Resolved],
  ): lav2.transaction_filter.TransactionFilter = {
    import lav1.transaction_filter.{Filters, InterfaceFilter, InclusiveFilters}

    val (templateIds, interfaceIds) = ResolvedQuery.partition(contractTypeIds)
    val filters = Filters(
      Some(
        InclusiveFilters(
          templateIds = templateIds.map(apiIdentifier),
          interfaceFilters = interfaceIds.map(interfaceId =>
            InterfaceFilter(
              interfaceId = Some(apiIdentifier(interfaceId)),
              includeInterfaceView = true,
            )
          ),
        )
      )
    )

    lav2.transaction_filter.TransactionFilter(
      domain.Party.unsubst((parties: Set[domain.Party]).toVector).map(_ -> filters).toMap
    )
  }

}
