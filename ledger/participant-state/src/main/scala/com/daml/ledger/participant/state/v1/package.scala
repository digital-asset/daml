// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value

package object v1 {
  // FIXME(JM): Use the DAML-LF "SimpleString" where applicable?

  /** Identifier for the ledger, MUST match regexp [a-zA-Z0-9-]. */
  type LedgerId = String

  /** Identifiers for transactions, MUST match regexp [a-zA-Z0-9-]. */
  type TransactionId = String

  /** Identifiers used to correlate submission with results, MUST match regexp [a-zA-Z0-9-]. */
  type CommandId = String

  /** Identifiers used for correlating submission with a workflow,  match regexp [a-zA-Z0-9-]. */
  type WorkflowId = String

  /** Identifiers for submitting client applications, MUST match regexp [a-zA-Z0-9-]. */
  type ApplicationId = String

  /** Identifiers for nodes in a transaction. */
  type NodeId = Transaction.NodeId

  /** Identifiers for packages. */
  type PackageId = Ref.PackageId

  /** Identifiers for parties, MUST match regexp [a-zA-Z0-9-]. */
  type Party = String

  /** Update identifier used to identify positions in the stream of updates.
    * The update identifier is a prefix-free monotonically increasing vector of
    * integers. */
  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  final case class UpdateId(xs: Array[Int]) {
    override def toString: String =
      xs.mkString("-")
  }

  /** The type for an yet uncommitted transaction with relative contact
    *  identifiers, submitted via 'submitTransaction'.
    */
  type SubmittedTransaction = Transaction.Transaction

  /** The type for a committed transaction. */
  type CommittedTransaction =
    GenTransaction[NodeId, Value.AbsoluteContractId, Value.VersionedValue[Value.AbsoluteContractId]]

  type AbsoluteContractInst =
    Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]]

  /** TODO (SM): expand this into a record. Mostly concern time. */
  type Configuration = String

  /** Information provided by the submitter of changes submitted to the ledger.
    *
    * Note that this is used for party-originating changes only. They are
    * usually issued via the Ledger API.
    *
    * @param submitter: the party that submitted the change.
    *
    * @param applicationId: an identifier for the DAML application that
    *   submitted the command. This is used for monitoring and to allow DAML
    *   applications subscribe to their own submissions only.
    *
    * @param commandId: a submitter provided identifier that he can use to
    *   correlate the stream of changes to the participant state with the
    *   changes he submitted.
    *
    * @param maxRecordTime: the maximum record time (inclusive) until which
    *   the submitted change can be validly added to the ledger. This is used
    *   by DAML applications to deduce from the record time reported by the
    *   ledger whether a change that they submitted has been lost in transit.
    *
    */
  final case class SubmitterInfo(
      submitter: Party,
      applicationId: ApplicationId,
      commandId: CommandId,
      maxRecordTime: Timestamp,
  )

  /** Meta-data of a transaction visible to all parties that can see a part of
    * the transaction.
    *
    * @param ledgerEffectiveTime: the submitter-provided time at which the
    *   transaction should be interpreted. This is the time returned by the
    *   DAML interpreter on a `getTime :: Update Time` call. See the docs on
    *   [[Update.TransactionAccepted]] for how it relates to the notion of
    *   `recordTime`.
    *
    * @param workflowId: a submitter-provided identifier used for monitoring
    *   and to traffic-shape the work handled by DAML applications
    *   communicating over the ledger.
    *
    */
  final case class TransactionMeta(ledgerEffectiveTime: Timestamp, workflowId: String)

}
