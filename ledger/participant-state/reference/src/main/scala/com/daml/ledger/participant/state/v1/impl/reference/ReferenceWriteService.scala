// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1.impl.reference

import com.daml.ledger.participant.state.v1

// FIXME (SM): why not implement that interface directly on 'Ledger'?
class ReferenceWriteService(ledger: Ledger) extends v1.WriteService {

  override def submitTransaction(
      submitterInfo: v1.SubmitterInfo,
      transactionMeta: v1.TransactionMeta,
      transaction: v1.SubmittedTransaction): Unit =
    ledger.submitTransaction(submitterInfo, transactionMeta, transaction)

}
