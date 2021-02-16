package com.daml.platform.store.dao.events

import java.time.Instant

import com.daml.ledger.{TransactionId, WorkflowId}
import com.daml.ledger.participant.state.v1.{
  CommittedTransaction,
  DivulgedContract,
  Offset,
  SubmitterInfo,
}
import com.daml.ledger.participant.state.v1.Update.TransactionAccepted
import com.daml.lf.transaction.BlindingInfo

final case class TransactionEntry(
    submitterInfo: Option[SubmitterInfo],
    workflowId: Option[WorkflowId],
    transactionId: TransactionId,
    ledgerEffectiveTime: Instant,
    offset: Offset,
    transaction: CommittedTransaction,
    divulgedContracts: Iterable[DivulgedContract],
    blindingInfo: Option[BlindingInfo],
)

object TransactionEntry {
  def apply(offset: Offset, transactionAccepted: TransactionAccepted): TransactionEntry = {
    import transactionAccepted._
    new TransactionEntry(
      optSubmitterInfo,
      transactionMeta.workflowId,
      transactionId,
      transactionMeta.ledgerEffectiveTime.toInstant,
      offset,
      transaction,
      divulgedContracts,
      blindingInfo,
    )
  }
}
