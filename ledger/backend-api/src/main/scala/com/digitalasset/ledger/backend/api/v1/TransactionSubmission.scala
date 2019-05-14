// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.backend.api.v1

import java.time.Instant

import com.digitalasset.daml.lf.transaction.BlindingInfo

/** A transaction and the meta-data necessary for submitting it to a ledger.
  *
  * @param maximumRecordTime:
  *   The maximum record time for this transaction to be committed to the
  *   ledger. The [[TransactionSubmission]] will never result in an
  *   [[LedgerSyncEvent.AcceptedTransaction]] event with a record time strictly larger than
  *   its maximum record time.
  *
  *   Submitting applications can use this guarantee together with the
  *   guarantee that [[LedgerBackend.ledgerSyncEvents]] have monotonically
  *   increasing 'recordTime' stamps to detect timed-out command submissions.
  *
  *
  * @param ledgerEffectiveTime:
  *   the ledger-effective time that was used to interpret the command into a
  *   transaction.
  *
  * @param transaction: the transaction that was accepted as committed.
  *
  * @param blindingInfo:
  *   Information about who can see what node in the transaction combined
  *   with information about what additional contracts must be divulged to
  *   recipients of this transaction to make the transaction self-contained.
  *   See https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#divulgence-when-non-stakeholders-see-contracts
  *   for background on divulgence.
  *
  *   TODO (SM): in LedgerBackend API V2 - find a way to implement the tricky
  *   divulgence logic in the ledger-api-server instead of inside every
  *   [[LedgerBackend]].
  *
  * @param workflowId:
  *   The on-ledger workflow id that this transction advances. It is used
  *   coordinated fashion by all parties participating in the workflow.
  *
  * @param submitter:
  *   the party that is submitting the transaction.
  *
  * @param applicationId:
  *   An identifier to identify the DAML application that submitted
  *   this transaction. Used for monitoring.
  *
  * @param commandId:
  *   A unique identifier of the command that produced the transaction. Used
  *   by the submitter to correlate the submission of a command with the
  *   acceptance or rejection of a transaction.
  *
  *
  * TODO (SM): in LedgerBackend API V2: combine the fields shared with
  * [[LedgerSyncEvent.AcceptedTransaction]] into their own types and clarify
  * their relation.
  *
  */
case class TransactionSubmission(
    commandId: String,
    workflowId: String,
    submitter: Party,
    ledgerEffectiveTime: Instant,
    maximumRecordTime: Instant,
    applicationId: String,
    blindingInfo: BlindingInfo,
    transaction: SubmittedTransaction)
