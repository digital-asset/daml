// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.util.concurrent.CompletionStage

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
  * As of now there are three methods for changing the state of a DAML ledger:
  * - submitting a transaction using [[WriteService!.submitTransaction]]
  * - allocating a new party using [[WritePartyService!.allocateParty]]
  * - uploading a new package using [[WritePackagesService!.uploadPackages]]
  *
  */
trait WriteService extends WritePackagesService with WritePartyService {

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
    * provided in the submission.
    *
    * In order to deal with potentially lost commands, the underlying
    * synchronisation layer must implement command-id de-duplication logic
    * based on the (command-id, command-id-deduplication-expiry) in order to
    * support application crash recovery.
    *
    * A note on ledger effective time: Transactions are submitted together
    * with a `ledgerEffectiveTime` provided as part of the
    * `transactionMeta` information. The ledger-effective time is used by the
    * DAML Engine to resolve calls to the `getTime :: Update Time`
    * function. Letting the submitter freely choose the ledger-effective time
    * is though a problem for the other stakeholders in the contracts affected
    * by the submitted transaction. The submitter can in principle choose to
    * submit transactions that are effective far in the past or future
    * relative to the wall-clock time of the other participants. This gives
    * the submitter an unfair advantage and make the semantics of `getTime`
    * quite surprising.
    *
    * However, time is a local property and in a distributed system, there is
    * no single time. Therefore, we treat `getTime` as an approximate property
    * and assume that the underlying sychronisation layer has some criteria to
    * decide on how much clock skew it is willing to accept.
    *
    * A consequence of that is that we do not guarantee transactions to be
    * ordered by LET on the event stream.
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
}
