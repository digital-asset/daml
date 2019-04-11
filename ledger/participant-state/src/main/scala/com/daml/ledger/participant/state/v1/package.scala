// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
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
  * The interfaces are optimized for easy implementation. The
  * [[v1.WriteService]] interface contains the methods for changing the
  * participant state (and potentially the state of the DAML ledger), which
  * all ledger participants must support. These methods are for example
  * exposed via the DAML Ledger API. Actual ledger participant implementations
  * likely support more implementation-specific methods. They are however not
  * exposed via the DAML Ledger API. The [[v1.ReadService]] interface contains
  * the one method [[v1.ReadService.stateUpdates]] to read the state of a ledger
  * participant. It represents the participant state as a stream of
  * [[v1.Update]]s to an initial participant state. The typcial consumer of this
  * method is a class that subscribes to this stream of [[v1.Update]]s and
  * reconstructs (a view of) the actual participant state. See the comments
  * on [[v1.Update]] and [[v1.ReadService.stateUpdates]] for details about the kind
  * of updates and the guarantees given to consumers of the stream of
  * [[v1.Update]]s.
  *
  */
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

  /** Offsets into streams with hierarchical addressing.
    *
    * We use these [[Offsets]]'s to address changes to the particpant state.
    * We allow for array of [[Int]] to allow for hierarchical adddresses.
    * These [[Int]] values are expected to be positive. Offsets are ordered by
    * lexicographic ordering of the array elements.
    *
    * A typical usecase for [[Offset]]s would be addressing a transaction in a
    * blockchain by `[<blockheight>, <transactionId>]`. Depending on the
    * structure of the underlying ledger these offsets are more or less
    * nested, which is why we use an array of [[Int]]s. The expectation is
    * though that there are usually few elements in the array.
    *
    */
  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  final case class Offset(private val xs: Array[Int]) {
    override def toString: String =
      xs.mkString("-")

    def components: Iterable[Int] = xs
  }
  implicit object Offset extends Ordering[Offset] {

    /** Create an offset from a string of form 1-2-3. Throws
      * NumberFormatException on misformatted strings.
      */
    def assertFromString(s: String): Offset =
      Offset(s.split('-').map(_.toInt))

    override def compare(x: Offset, y: Offset): Int =
      scala.math.Ordering.Iterable[Int].compare(x.xs, y.xs)
  }

  /** The type for a yet uncommitted transaction with relative contract
    *  identifiers, submitted via 'submitTransaction'.
    */
  type SubmittedTransaction = Transaction.Transaction

  /** The type for a committed transaction, which uses absolute contract
    *  identifiers.
    */
  type CommittedTransaction =
    GenTransaction[NodeId, Value.AbsoluteContractId, Value.VersionedValue[Value.AbsoluteContractId]]

  /** A contract instance with absolute contract identifiers. */
  type AbsoluteContractInst =
    Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]]

  /** TODO (SM): expand this into a record for time-limit configuration and an
   *  explanation of how it is intended to be implemented.
   *  https://github.com/digital-asset/daml/issues/385
   */
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
