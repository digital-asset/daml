// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.daml.ledger.sandbox.bridge.validate.ConflictCheckingLedgerBridge.{KeyInputs, UpdatedKeys}
import com.daml.ledger.sandbox.domain.Submission
import com.daml.lf.data.Ref
import com.daml.lf.transaction.BlindingInfo
import com.daml.platform.store.appendonlydao.events

// A submission that has been prepared for conflict checking
sealed trait PreparedSubmission extends Product with Serializable {
  def submission: Submission
}

// A transaction submission bundled with all its precomputed effects.
final case class PreparedTransactionSubmission(
    keyInputs: KeyInputs,
    inputContracts: Set[events.ContractId],
    updatedKeys: UpdatedKeys,
    consumedContracts: Set[events.ContractId],
    blindingInfo: BlindingInfo,
    transactionInformees: Set[Ref.Party],
    submission: Submission.Transaction,
) extends PreparedSubmission

// A no-op prepared submission for update types that do not need
// preparation for conflict checking.
final case class NoOpPreparedSubmission(submission: Submission) extends PreparedSubmission
