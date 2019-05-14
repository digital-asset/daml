// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.backend.api

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId

/** Version 1 of the Ledger Backend API.
  *
  * The main class is [[com.digitalasset.ledger.backend.api.v1.LedgerBackend]]. It abstracts over different means for
  * reading and writing from a ledger; and thereby allow providing the DAML Ledger API on a variety of backends.
  * See the documentation of [[com.digitalasset.ledger.backend.api.v1.LedgerBackend]] for details.
  *
  * We are in the process of simplifying and streamlining the implementation of LedgerBackends. We aim to do that by
  * publishing additional versions of this API side-by-side with the existing ones and migration information. Similar to
  * how Microsoft's DirectX evolved.
  *
  * The V1 API is stable insofar as it will not change, but it may be deprecated at any time in favor of a V2 version.
  * If you use it make sure that your stability needs and migration plans are known to DA.
  *
  */
package object v1 {

  /** Identifiers for transactions, MUST match regexp [a-zA-Z0-9-]. */
  type TransactionId = String

  /** Identifiers used to correlate submission with results, MUST match regexp [a-zA-Z0-9-]. */
  type CommandId = String

  /** Identifiers for nodes in a transaction, MUST match regexp [a-zA-Z0-9-]. */
  type NodeId = String

  /** Identifiers for [[LedgerSyncEvent]]'s, MUST match regexp [a-zA-Z0-9-]. */
  type LedgerSyncOffset = String

  /** Identifiers for parties */
  type Party = Ref.Party

  /** The type for an yet uncommitted transaction with relative contact
    *  identifiers, submitted via 'submitTransaction'.
    */
  type SubmittedTransaction = Transaction.Transaction

  /** The type for a committed transaction, streamed as part of a
    *  [[com.digitalasset.ledger.backend.api.v1.LedgerSyncEvent.AcceptedTransaction]] event on the [[LedgerBackend.ledgerSyncEvents]]
    *  stream. */
  type CommittedTransaction =
    GenTransaction.WithTxValue[NodeId, AbsoluteContractId]
}
