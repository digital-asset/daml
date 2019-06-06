// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value

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
  * This is version v2 of the [[com.daml.ledger.participant.state]]
  * interfaces.
  * - v1 is kept as is to serve the needs of //ledger/api-server-damlonx.
  * It will be deprecated and dropped as soon as Sandbox ledger API server
  * becomes a common backbone of all daml-on-x implementations.
  * - v2 is under development and evolving to serve the needs of merging the
  * Sandbox ledger API server with the DAML on X API server as per
  * https://github.com/digital-asset/daml/issues/1273
  *
  * The interfaces are optimized for easy implementation. The
  * [[v2.WriteService]] interface contains the methods for changing the
  * participant state (and potentially the state of the DAML ledger), which
  * all ledger participants must support. These methods are for example
  * exposed via the DAML Ledger API. Actual ledger participant implementations
  * likely support more implementation-specific methods. They are however not
  * exposed via the DAML Ledger API. The [[v2.ReadService]] interface contains
  * the one method [[v2.ReadService.stateUpdates]] to read the state of a ledger
  * participant. It represents the participant state as a stream of
  * [[v2.Update]]s to an initial participant state. The typical consumer of this
  * method is a class that subscribes to this stream of [[v2.Update]]s and
  * reconstructs (a view of) the actual participant state. See the comments
  * on [[v2.Update]] and [[v2.ReadService.stateUpdates]] for details about the kind
  * of updates and the guarantees given to consumers of the stream of
  * [[v2.Update]]s.
  *
  */
package object v2 {

  /** Identifier for the ledger, MUST match regexp [a-zA-Z0-9-]. */
  type LedgerId = String

  /** Identifiers for transactions.
    * Currently unrestricted unicode (See issue #398). */
  type TransactionId = Ref.TransactionIdString

  /** Identifiers used to correlate submission with results.
    * Currently unrestricted unicode (See issue #398). */
  type CommandId = Ref.LedgerString

  /** Identifiers used for correlating submission with a workflow.
    * Currently unrestricted unicode (See issue #398).  */
  type WorkflowId = Ref.LedgerString

  /** Identifiers for submitting client applications.
    * Currently unrestricted unicode (See issue #398). */
  type ApplicationId = Ref.LedgerString

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
