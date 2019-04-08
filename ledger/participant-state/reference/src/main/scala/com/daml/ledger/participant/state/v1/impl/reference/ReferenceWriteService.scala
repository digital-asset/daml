// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1.impl.reference

import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{StateId, SubmittedTransaction, WriteService}

class ReferenceWriteService(ledger: Ledger) extends WriteService {

  override def submitTransaction(
      stateId: StateId,
      submitterInfo: v1.SubmitterInfo,
      transactionMeta: v1.TransactionMeta,
      transaction: SubmittedTransaction): Unit =
    // FIXME(JM): Validate stateId.
    ledger.submitTransaction(submitterInfo, transactionMeta, transaction)

}
