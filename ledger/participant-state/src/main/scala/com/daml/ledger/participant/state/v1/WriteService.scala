package com.daml.ledger.participant.state.v1

trait WriteService {
  def submitTransaction(submitterInfo: SubmitterInfo,
                        transactionMeta: TransactionMeta,
                        transaction: SubmittedTransaction): Unit

}
