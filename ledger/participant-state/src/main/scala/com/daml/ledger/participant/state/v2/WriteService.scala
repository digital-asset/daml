// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.util.concurrent.CompletionStage

import com.daml.ledger.api.health.ReportsHealth
import com.daml.lf.data.Time
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.telemetry.TelemetryContext

/** An interface to change a ledger via a participant.
  * '''Please note that this interface is unstable and may significantly change.'''
  *
  * The methods in this interface are all methods that are supported
  * *uniformly* across all ledger participant implementations. Methods for
  * uploading packages, on-boarding parties, and changing ledger-wide
  * configuration are specific to a ledger and therefore to a participant
  * implementation. Moreover, these methods usually require admin-level
  * privileges, whose granting is also specific to a ledger.
  *
  * If a ledger is run for testing only, there is the option for quite freely
  * allowing the on-boarding of parties and uploading of packages. There are
  * plans to make this functionality uniformly available: see the roadmap for
  * progress information https://github.com/digital-asset/daml/issues/121.
  *
  * As of now there are five methods for changing the state of a Daml ledger:
  * - submitting a transaction using [[WriteService!.submitTransaction]]
  * - recording the rejection of a command submission using [[WriteService!.rejectSubmission]]
  * - allocating a new party using [[WritePartyService!.allocateParty]]
  * - uploading a new package using [[WritePackagesService!.uploadPackages]]
  * - pruning a participant ledger using [[WriteParticipantPruningService!.prune]]
  */
trait WriteService
    extends WritePackagesService
    with WritePartyService
    with WriteConfigService
    with WriteParticipantPruningService
    with ReportsHealth {

  /** Submit a transaction for acceptance to the ledger.
    *
    * This method must be thread-safe.
    *
    * The result of the transaction submission is communicated asynchronously
    * via a [[ReadService]] implementation backed by the same participant
    * state as this [[WriteService]]. Successful transaction acceptance is
    * communicated using a [[Update.TransactionAccepted]] message. Failed
    * transaction acceptance is communicated when possible via a
    * [[Update.CommandRejected]] message referencing the same `submitterInfo` as
    * provided in the submission. There can be failure modes where a
    * transaction submission is lost in transit, and no [[Update.CommandRejected]] is
    * generated. See the comments on [[ReadService.stateUpdates]] for further details.
    *
    * A note on ledger time and record time: transactions are
    * submitted together with a `ledgerTime` provided as part of the
    * `transactionMeta` information. The ledger time is used by the
    * Daml Engine to resolve calls to the `getTime :: Update Time`
    * function. Letting the submitter freely choose the ledger time
    * is though a problem for the other stakeholders in the contracts affected
    * by the submitted transaction. The submitter can in principle choose to
    * submit transactions that are effective far in the past or future
    * relative to the wall-clock time of the other participants. This gives
    * the submitter an unfair advantage and make the semantics of `getTime`
    * quite surprising. We've chosen the following solution to provide useful
    * guarantees for contracts relying on `getTime`.
    *
    * The ledger is charged with (1) associating record-time stamps to accepted
    * transactions and (2) to provide a guarantee on the maximal skew between the
    * ledger effective time and the record time stamp associated to an
    * accepted transaction. The ledger is also expected to provide guarantees
    * on the distribution of the maximal skew between record time stamps on
    * accepted transactions and the wall-clock time at delivery of accepted transactions to a ledger
    * participant. Thereby providing ledger participants with a guarantee on the
    * maximal skew between the ledger effective time of an accepted
    * transaction and the wall-clock time at delivery to these participants.
    *
    * Concretely, we typically expect the allowed skew between record time and
    * ledger time to be in the minute range. Thereby leaving ample
    * time for submitting and validating large transactions before they are
    * timestamped with their record time.
    *
    * The [[WriteService]] is responsible for deduplicating commands
    * with the same [[SubmitterInfo.changeId]] within the [[SubmitterInfo.deduplicationPeriod]].
    *
    * @param submitterInfo               the information provided by the submitter for
    *                                    correlating this submission with its acceptance or rejection on the
    *                                    associated [[ReadService]].
    * @param transactionMeta             the meta-data accessible to all consumers of the transaction.
    *                                    See [[TransactionMeta]] for more information.
    * @param transaction                 the submitted transaction. This transaction can contain local
    *                                    contract-ids that need suffixing. The participant state may have to
    *                                    suffix those contract-ids in order to guaranteed their global
    *                                    uniqueness. See the Contract Id specification for more detail
    *                                    daml-lf/spec/contract-id.rst.
    * @param estimatedInterpretationCost Estimated cost of interpretation that may be used for
    *                                    handling submitted transactions differently.
    * @param telemetryContext            Implicit context for tracing.
    */
  def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult]

  /** Record the rejection of a command submission on the ledger.
    *
    * This method must be thread-safe.
    *
    * This method is used by the Ledger API server to deliver interpretation failures as definitive answers to the
    * Ledger API client via completions; thereby allowing the Ledger API client to safely decide to not retry
    * the command if desired.
    *
    * The result is communicated asynchronously via a [[ReadService]] implementation backed by the same participant
    * state as this [[WriteService]]. Successful recording is communicated using a [[Update.CommandRejected]]
    * with [[SubmitterInfo]] and [[Update.CommandRejected.definiteAnswer]].
    * If the recording as a rejection fails (e.g., due to deduplication),
    * the failure should be communicated using a [[Update.CommandRejected]] with [[SubmitterInfo]]
    * and not [[Update.CommandRejected.definiteAnswer]].
    *
    * Recorded rejections fall under the deduplication guarantees described in [[ReadService.stateUpdates]].
    *
    * @param submitterInfo the information provided by the submitter for correlating this submission
    *                      with its rejection on the associated [[ReadService]].
    * @param submissionTime the submission time of the rejected submission like in [[TransactionMeta.submissionTime]]
    * @param reason The rejection reason to be included in the [[Update.CommandRejected]]
    * @param telemetryContext Implicit context for tracing.
    */
  def rejectSubmission(
      submitterInfo: SubmitterInfo,
      submissionTime: Time.Timestamp,
      reason: com.google.rpc.status.Status,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult]
}
