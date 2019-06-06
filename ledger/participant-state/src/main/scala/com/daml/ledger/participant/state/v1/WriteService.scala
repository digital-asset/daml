// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.util.concurrent.CompletionStage

import com.digitalasset.daml_lf.DamlLf.Archive

/** An interface to change a ledger via a participant.
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
  * As of now there are two methods for changing the state of a DAML ledger:
  * - submitting a transaction using [[WriteService!.submitTransaction]].
  * - allocating a new party using [[WriteService!.allocateParty]]
  *
  */
trait WriteService {

  /** Submit a transaction for acceptance to the ledger.
    *
    * This method must be thread-safe, not throw, and not block on IO. It is
    * though allowed to perform significant computation. The expectation is
    * that callers call this method on a thread dedicated to getting this transaction
    * ready for acceptance to the ledger, which typically requires some
    * preparation steps (decomposition, serialization) by the implementation
    * of the [[WriteService]].
    *
    * The result of the transaction submission is communicated asynchronously
    * via a [[ReadService]] implementation backed by the same participant
    * state as this [[WriteService]]. Successful transaction acceptance is
    * communicated using a [[Update.TransactionAccepted]] message. Failed
    * transaction acceptance is communicated when possible via a
    * [[Update.CommandRejected]] message referencing the same `submitterInfo` as
    * provided in the submission. There can be failure modes where a
    * transaction submission is lost in transit, and no [[Update.CommandRejected]] is
    * generated. These failures are communicated via [[Update.Heartbeat]]s signalling
    * that the `maximumRecordTime` provided in the submitter info has been
    * exceeded. See the comments on [[ReadService.stateUpdates]] for further details.
    *
    * A note on ledger effective time and record time: transactions are
    * submitted together with a `ledgerEffectiveTime` provided as part of the
    * `transactionMeta` information. The ledger-effective time is used by the
    * DAML Engine to resolve calls to the `getTime :: Update Time`
    * function. Letting the submitter freely choose the ledger-effective time
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
    * ledger effective time to be in the minute range. Thereby leaving ample
    * time for submitting and validating large transactions before they are
    * timestamped with their record time.
    *
    * @param submitterInfo   : the information provided by the submitter for
    *                        correlating this submission with its acceptance or rejection on the
    *                        associated [[ReadService]].
    * @param transactionMeta : the meta-data accessible to all consumers of the
    *                        transaction. See [[TransactionMeta]] for more information.
    * @param transaction     : the submitted transaction. This transaction can
    *                        contain contract-ids that are relative to this transaction itself.
    *                        These are used to refer to contracts created in the transaction
    *                        itself. The participant state implementation is expected to convert
    *                        these into absolute contract-ids that are guaranteed to be unique.
    *                        This typically happens after a transaction has been assigned a
    *                        globally unique id, as then the contract-ids can be derived from that
    *                        transaction id.
    *
    * @return an async result of a SubmissionResult
    */
  def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): CompletionStage[SubmissionResult]

  /** Upload a collection of DAML-LF packages to the ledger.
    *
    * This method must be thread-safe, not throw, and not block on IO. It is
    * though allowed to perform significant computation.
    *
    * The result of the archives upload is communicated synchronously.
    * TODO: consider also providing an asynchronous response in a similar
    * manner as it is done for transaction submission. It is possible that
    * in some implementations, upload will fail due to authorization etc.
    *
    * Successful archives upload will result in a [[Update.PublicPackagesUploaded]]
    * message. See the comments on [[ReadService.stateUpdates]] and [[Update]] for
    * further details.
    *
    * @param archives        : DAML-LF packages to be uploaded to the ledger.
    * @param sourceDescription : the description of the packages provided by the
    *                            participant implementation.
    *
    * @return an async result of a SubmissionResult
    */
  def uploadPackages(
      archives: List[Archive],
      sourceDescription: String): CompletionStage[SubmissionResult]

  /**
    * Adds a new party to the set managed by the ledger.
    *
    * Caller specifies a party identifier suggestion, the actual identifier
    * allocated might be different and is implementation specific.
    *
    * In particular, a ledger may:
    * - Disregard the given hint and choose a completely new party identifier
    * - Construct a new unique identifier from the given hint, e.g., by appending a UUID
    * - Use the given hint as is, and reject the call if such a party already exists
    *
    * @param hint A party identifier suggestion
    * @param displayName A human readable name of the new party
    *
    * @return an async result of a SubmissionResult
    */
  def allocateParty(
      hint: Option[String],
      displayName: Option[String]
  ): CompletionStage[SubmissionResult]
}
