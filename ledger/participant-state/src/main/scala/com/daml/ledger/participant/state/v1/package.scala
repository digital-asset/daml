// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state

import com.daml.lf.data.Ref
import com.daml.lf.transaction.{GenTransaction, Transaction}
import com.daml.lf.value.Value

/** Interfaces to read from and write to an (abstract) participant state.
  *
  * A DAML ledger participant is code that allows to actively participate in
  * the evolution of a shared DAML ledger. Each such participant maintains a
  * particular view onto the state of the DAML ledger. We call this view the
  * participant state.
  *
  * Actual implementations of a DAML ledger participant will likely maintain
  * more state than what is exposed through the interfaces in this package,
  * which is why we talk about an abstract participant state. It abstracts
  * over the different implementations of DAML ledger participants.
  *
  * The interfaces are optimized for easy implementation. The
  * [[v1.WriteService]] interface contains the methods for changing the
  * participant state (and potentially the state of the DAML ledger), which
  * all ledger participants must support. These methods are for example
  * exposed via the DAML Ledger API. Actual ledger participant implementations
  * likely support more implementation-specific methods. They are however not
  * exposed via the DAML Ledger API. The [[v1.ReadService]] interface contains
  * the one method [[v1.ReadService.stateUpdates]] to read the state of a ledger
  * participant. It represents the participant state as a stream of
  * [[v1.Update]]s to an initial participant state. The typical consumer of this
  * method is a class that subscribes to this stream of [[v1.Update]]s and
  * reconstructs (a view of) the actual participant state. See the comments
  * on [[v1.Update]] and [[v1.ReadService.stateUpdates]] for details about the kind
  * of updates and the guarantees given to consumers of the stream of
  * [[v1.Update]]s.
  *
  * We provide a reference implementation of a participant state in
  * [[com.daml.ledger.api.server.damlonx.reference.v2.ReferenceServer]]. There we
  * model an in-memory ledger, which has by construction a single participant,
  * which hosts all parties. See its comments for details on how that is done,
  * and how its implementation can be used as a blueprint for implementing
  * your own participant state.
  *
  * We do expect the interfaces provided in
  * [[com.daml.ledger.participant.state]] to evolve, which is why we
  * provide them all in the
  * [[com.daml.ledger.participant.state.v1]] package.  Where possible
  * we will evolve them in a backwards compatible fashion, so that a simple
  * recompile suffices to upgrade to a new version. Where that is not
  * possible, we plan to introduce new version of this API in a separate
  * package and maintain it side-by-side with the existing version if
  * possible. There can therefore potentially be multiple versions of
  * participant state APIs at the same time. We plan to deprecate and drop old
  * versions on separate and appropriate timelines.
  */
package object v1 {

  /** Identifier for the ledger, MUST match regexp [a-zA-Z0-9-]. */
  type LedgerId = String

  /** Identifier for the participant, MUST match regexp [a-zA-Z0-9-]. */
  val ParticipantId: Ref.ParticipantId.type = Ref.ParticipantId
  type ParticipantId = Ref.ParticipantId

  /** Identifiers for transactions. */
  val TransactionId: Ref.LedgerString.type = Ref.LedgerString
  type TransactionId = Ref.LedgerString

  /** Identifiers used to correlate submission with results. */
  val CommandId: Ref.LedgerString.type = Ref.LedgerString
  type CommandId = Ref.LedgerString

  /** Identifiers used for correlating submission with a workflow. */
  val WorkflowId: Ref.LedgerString.type = Ref.LedgerString
  type WorkflowId = Ref.LedgerString

  /** Identifiers for submitting client applications. */
  val ApplicationId: Ref.LedgerString.type = Ref.LedgerString
  type ApplicationId = Ref.LedgerString

  /** Identifiers used to correlate admin submission with results. */
  val SubmissionId: Ref.LedgerString.type = Ref.LedgerString
  type SubmissionId = Ref.LedgerString

  /** Identifiers for nodes in a transaction. */
  type NodeId = Transaction.NodeId

  /** Identifiers for packages. */
  type PackageId = Ref.PackageId

  /** Identifiers for parties. */
  type Party = Ref.Party

  /** A transaction with relative and absolute contract identifiers.
    *
    *  See [[WriteService.submitTransaction]] for details.
    */
  type SubmittedTransaction = Transaction.Transaction

  /** A transaction with absolute contract identifiers only.
    *
    * Used to communicate transactions that have been accepted to the ledger.
    * See [[WriteService.submitTransaction]] for details on relative and
    * absolute contract identifiers.
    */
  type CommittedTransaction =
    GenTransaction.WithTxValue[NodeId, Value.AbsoluteContractId]

  /** A contract instance with absolute contract identifiers only. */
  type AbsoluteContractInst =
    Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]]

}
