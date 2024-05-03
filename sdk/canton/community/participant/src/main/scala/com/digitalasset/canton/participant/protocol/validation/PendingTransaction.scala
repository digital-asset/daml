// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.EngineController.EngineAbortStatus
import com.digitalasset.canton.participant.protocol.ProcessingSteps.PendingRequestData
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.{RequestCounter, SequencerCounter}

/** Storing metadata of pending transactions required for emitting transactions on the sync API. */
final case class PendingTransaction(
    freshOwnTimelyTx: Boolean,
    requestTime: CantonTimestamp,
    override val requestCounter: RequestCounter,
    override val requestSequencerCounter: SequencerCounter,
    transactionValidationResult: TransactionValidationResult,
    override val mediator: MediatorGroupRecipient,
    override val locallyRejectedF: FutureUnlessShutdown[Boolean],
    override val abortEngine: String => Unit,
    override val engineAbortStatusF: FutureUnlessShutdown[EngineAbortStatus],
) extends PendingRequestData {

  val requestId: RequestId = RequestId(requestTime)
  override def rootHashO: Option[RootHash] = Some(
    transactionValidationResult.transactionId.toRootHash
  )
}
