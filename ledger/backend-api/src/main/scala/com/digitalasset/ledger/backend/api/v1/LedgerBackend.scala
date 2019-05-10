// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.backend.api.v1

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.ledger.backend.api.v1.LedgerSyncEvent.AcceptedTransaction

import scala.concurrent.Future

/** An instance of the [[LedgerBackend]] trait allows to read from and write
  * to a DAML Ledger.
  *
  * The purpose of the [[LedgerBackend]] trait is to decouple the
  * implementation of the ledger-api-server, which provides the user-facing
  * DAML Ledger API, from the implementation of the underlying storage and
  * synchronization mechanism. The ledger-api-server can thereby be used to
  * offer the DAML Ledger API for many different distributed ledger and
  * database technologies.
  *
  * The semantics of DAML Ledgers are specified as part of the DA Ledger
  * model: https://docs.daml.com/concepts/ledger-model/index.html. We assume
  * knowledge of the concepts explained therein for the remainder of this
  * documentation. We also recommend going through the Getting Started Guide
  * in the DAML SDK to get a feel for how a DAML Application works:
  * https://docs.daml.com/getting-started/installation.html
  *
  * The [[LedgerBackend]] trait is structured such that it can also be
  * implemented for distributed ledgers where each node has its own local view
  * on the state of the ledger and on what transactions have been accepted.
  *
  * The [[LedgerBackend]] trait exposes changes to a Participant node's state
  * as a stream of [[LedgerSyncEvent]]s. Not all changes are required to be
  * exposed. Only the ones observable at the level of the DAML Ledger API. Use
  * [[LedgerBackend.ledgerSyncEvents]] to consume this stream. Every
  * [[LedgerSyncEvent]] has an associated [[LedgerSyncOffset]], which serves
  * as a Participant-node-local address of that event. This allows consuming
  * the [[LedgerSyncOffset.ledgerSyncEvents]] stream starting after a specific
  * offset.
  *
  * Participant nodes often host the data for multiple DAML Parties; i.e.,
  * they run in a multi-tenancy setup. The privacy model for DAML
  * (https://docs.daml.com/concepts/ledger-model/ledger-privacy.html) is
  * though defined on a per-party basis. This is why all methods that read
  * privacy-sensitive data from a Participant node take the requesting
  * party(ies) as an argument; see e.g., [[LedgerBackend.ledgerSyncEvents]].
  * The implementors of [[LedgerBackend]] need to take care to properly filter
  * the returned data to only include the requesting parties view.
  *
  * TODO (SM): move the per-party filtering into the domain of the
  * ledger-api-server as part of the V2 version of the LedgerBackend API
  *
  * In principle, all reads from a Participant node's state can be performed
  * using [[LedgerBackend.ledgerSyncEvents]] and computing the desired result
  * over the returned stream. However this would not perform well, which is why
  * we added extra methods to the [[LedgerBackend]] where desirable.
  *
  * All writes to the ledger happen by submitting a transaction using
  * [[LedgerBackend.beginSubmission]] and calling [[SubmissionHandle.submit]]
  * on the returned [[SubmissionHandle]]. It is expected that a Participant
  * node coordinates with the other nodes constituting the ledger to commit
  * the submitted transaction and inform the relevant Participant nodes about
  * the newly committed transaction.
  *
  */
trait LedgerBackend extends AutoCloseable {

  /** Return the identifier of the Participant node's state that this
    * [[LedgerBackend]] reads from and writes to.
    *
    * This identifier is used by consumers of the DAML Ledger API to check
    * on reconnects to the Ledger API that they are connected to the same
    * ledger and can therefore expect to receive the same data on calls that
    * return append-only data. It is expected to be:
    * (1) immutable over the lifetime of a [[LedgerBackend]] instance,
    * (2) globally unique with high-probability,
    * (3) matching the regexp [a-zA-Z0-9]+.
    *
    * Implementations where Participant nodes share a global view on all
    * transactions in the ledger (e.g, via a blockchain) are expected to use
    * the same ledger-id on all Participant nodes. Implementations where
    * Participant nodes do not share a global view should ensure that the
    * different participant nodes use different ledger-ids.
    *
    * TODO(SM): find a better name than 'ledger-id'.
    */
  def ledgerId: String

  /** Begin the submission of a transaction to the ledger.
    *
    * Every write to the ledger is initiated with its own call to this
    * method. The returned [[SubmissionHandle]] is used by the DAML
    * interpreter to read from the ledger and construct a transaction. See
    * [[SubmissionHandle]] for details on its methods.
    *
    * This method SHOULD be light-weight on average. Implementors might
    * for example use a connection pool to avoid high setup costs for
    * connecting to its Participant node.
    */
  def beginSubmission(): Future[SubmissionHandle]

  /** Return the stream of ledger events starting from and including the given offset.
    *
    * @param offset : the ledger offset starting from which events should be streamed.
    *
    *               The stream only terminates if there was an error.
    *
    *               Two calls to this method with the same arguments are related such
    *               that
    *               (1) all events are delivered in the same order, but
    *               (2) [[LedgerSyncEvent.RejectedCommand]] and [[LedgerSyncEvent.Heartbeat]] events can be elided if their
    *               recordTime is equal to the preceding event.
    *               This rule provides implementors with the freedom to not persist
    *               [[LedgerSyncEvent.RejectedCommand]] and [[LedgerSyncEvent.Heartbeat]] events.
    *
    */
  def ledgerSyncEvents(offset: Option[LedgerSyncOffset] = None): Source[LedgerSyncEvent, NotUsed]

  /** Return a recent snapshot of the active contracts.
    *
    * It is up to the implementation to decide on what 'recent' means.
    * Consumers typically follow up on a call to this method with a call to
    * [[ledgerSyncEvents]] starting from the snapshot's offset to track
    * changes to that snapshot.
    *
    * TODO (SM): as part of the V2 API fix the problem that this will result in the create events
    * at an accepted-transaction at the latest offset being returned twice: once as part of the active-contract
    * snapshot and once as part of the first ledger-event returned by [[ledgerSyncEvents]].
    *
    * Semantically the method MUST return exactly the contracts for which
    * there was a 'Create' event and no
    * consuming 'Exercise' event in an [[AcceptedTransaction]] in the
    * [[ledgerSyncEvents]] for the 'requestingParties' starting from the
    * beginning until and including the offset at which the snapshot is
    * computed.
    *
    * Implementations are expected to serve this stream in time proportional
    * to its size.
    *
    */
  def activeContractSetSnapshot(): Future[(LedgerSyncOffset, Source[ActiveContract, NotUsed])]

  /** Return the current [[LedgerSyncOffset]].
    *
    * Implementations are expected to return an offset whose associated
    * [[LedgerSyncOffset]] has a 'recordTime' that is close to the wall-clock
    * time of the Participant node. This is what 'current' means for this
    * method.
    *
    * The typical use-case for this method is to use the returned offset as the
    * starting offset for [[ledgerSyncEvents]] stream to subscribe to recent
    * changes of the ledger.
    *
    */
  def getCurrentLedgerEnd: Future[LedgerSyncOffset]

  /** Looks up a transaction by its id. */
  def getTransactionById(transactionId: TransactionId): Future[Option[AcceptedTransaction]]
}
