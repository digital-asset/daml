// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  TransactionRejector,
}
import com.daml.ledger.participant.state.kvutils.committer.{StepContinue, StepResult}
import com.daml.ledger.participant.state.v1.RejectionReasonV0
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext

private[validation] object KeyMonotonicityValidation {

  /** LookupByKey nodes themselves don't actually fetch the contract.
    * Therefore we need to do an additional check on all contract keys
    * to ensure the referred contract satisfies the causal monotonicity invariant.
    * This could be reduced to only validate this for keys referred to by
    * NodeLookupByKey.
    */
  def checkContractKeysCausalMonotonicity(
      recordTime: Option[Timestamp],
      keys: Set[DamlStateKey],
      damlState: Map[DamlStateKey, DamlStateValue],
      transactionEntry: DamlTransactionEntrySummary,
      transactionRejector: TransactionRejector,
  )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
    val causalKeyMonotonicity = keys.forall { key =>
      val state = damlState(key)
      val keyActiveAt =
        Conversions
          .parseTimestamp(state.getContractKeyState.getActiveAt)
          .toInstant
      !keyActiveAt.isAfter(transactionEntry.ledgerEffectiveTime.toInstant)
    }
    if (causalKeyMonotonicity)
      StepContinue(transactionEntry)
    else
      transactionRejector.reject(
        transactionRejector.buildRejectionEntry(
          transactionEntry,
          RejectionReasonV0.InvalidLedgerTime("Causal monotonicity violated"),
        ),
        recordTime,
      )
  }
}
