// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.backend.api.v1

import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst, VersionedValue}

import scala.concurrent.Future

sealed abstract class SubmissionResult extends Product with Serializable

object SubmissionResult {

  /** The request has been received. */
  final case object Acknowledged extends SubmissionResult

  /** The system is overloaded, clients should back off exponentially */
  final case object Overloaded extends SubmissionResult

}

/**
  * A means to construct and submit a single transaction.
  *
  * The handle allows tracking calls to
  * [[SubmissionHandle.lookupActiveContract]] in order to double-check
  * their activeness on commit time.
  *
  * The [[SubmissionHandle]] moves into an invalid state after calling
  * [[SubmissionHandle.abort]] or [[SubmissionHandle.submit]]. On all
  * further calls to a method it MUST throw [[IllegalStateException]].
  */
trait SubmissionHandle {

  /**
    * Abort the submission.
    *
    * Please note that after calling [[abort]] the [[SubmissionHandle]]
    * is invalid -- see scaladoc for [[SubmissionHandle]].
    *
    * TODO (SM): in LedgerBackend API V2 - provide the necessary data
    * to inject a [[LedgerSyncEvent.RejectedCommand]] event into the
    * ledgerSyncEvents stream
    */
  def abort: Future[Unit]

  /**
    * Submit a transaction.
    *
    * This is expected to coordinate the committing of
    * the transaction into the ledger and create an
    * [[LedgerSyncEvent.AcceptedTransaction]] event in the
    * 'ledgerSyncEvents' streams of all Participant nodes hosting
    * parties affected by this transaction. A failed submission will
    * be evidenced as a [[LedgerSyncEvent.RejectedCommand]] on the
    * 'ledgerSyncEvents' stream.
    *
    * Please note that after calling [[submit]] the [[SubmissionHandle]]
    * is invalid -- see scaladoc for [[SubmissionHandle]].
    *
    * TODO(SM): in LedgerBackend API V2 -- move to a simpler transaction
    * submission model where the transaction itself captures all
    * dependencies on the state of the ledger.
    */
  def submit(submission: TransactionSubmission): Future[SubmissionResult]

  /**
    * Lookup an active contract using the supplied absolute contract ID.
    *
    * Activeness is relative to the potentially changing state of the
    * ledger at the call.
    *
    * Assume the function is called twice and does not return the same
    * result, then there are must be only two possibilities:
    * 1) first call returns None; second call returns (Some contract);
    *    i.e. contract created between the calls.
    * 2) first call returns (Some contract); second call returns None;
    *    i.e. contract archived between the calls.
    *
    * NOTE(FM): this function not always returning the same result is
    * due to the fact that we do not know that all our backends are
    * going to support transactions. However, note that this does not
    * prevent the DAML engine or in general DAML interpreters from
    * caching the results of this call while executing. This is since at
    * commit time the consistency of the transaction with what regards
    * to the underlying data store must be checked regardless. On the
    * contrary, the engine very much should cache results to achieve
    * repeatable reads on its own. However we currently do not: we
    * always look up absolute contract ids upstream. We should fix it to
    * record information about absolute contract ids that we've fetched
    * / archived to be more compatible with this interface.
    */
  def lookupActiveContract(submitter: Party, contractId: AbsoluteContractId)
    : Future[Option[ContractInst[VersionedValue[AbsoluteContractId]]]]

  /**
    * Lookup a contract by its key. The result of this function can
    * change given that the mapping from keys to absolute contract id
    * is mutable.
    *
    * Note that the same caveats regarding caching mentioned in
    * [[lookupActiveContract]] apply here. However, the DAML Engine
    * already stores information about global keys which concern the
    * current transaction, since it must do so to implement lookups
    * correctly -- consider the case where the user exercises an
    * absolute contract id: successive lookups of the key corresponding
    * the absolute contract id must return [[Nothing]].
    */
  def lookupContractKey(submitter: Party, key: GlobalKey): Future[Option[AbsoluteContractId]]
}
