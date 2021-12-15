// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge

import com.daml.ledger.sandbox.domain.Submission
import com.daml.lf.transaction.BlindingInfo
import com.daml.lf.transaction.Transaction.{KeyInput => TxKeyInput}
import com.daml.platform.store.appendonlydao.events
import com.daml.platform.store.appendonlydao.events.Key

// A submission that has been prepared for conflict checking
sealed trait PreparedSubmission extends Product with Serializable

// A transaction submission bundled with all its precomputed effects.
final case class PreparedTransactionSubmission(
    keyInputs: Map[Key, TxKeyInput],
    inputContracts: Set[events.ContractId],
    updatedKeys: Map[Key, Option[events.ContractId]],
    consumedContracts: Set[events.ContractId],
    blindingInfo: BlindingInfo,
    originalSubmission: Submission.Transaction,
) extends PreparedSubmission

// A no-op prepared submission for update types that do not need
// preparation for conflict checking.
final case class NoOpPreparedSubmission(submission: Submission) extends PreparedSubmission
