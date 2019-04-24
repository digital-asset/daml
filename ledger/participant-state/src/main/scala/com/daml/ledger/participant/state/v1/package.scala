// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.platform.services.time.TimeModel

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
  * [[com.daml.ledger.participant.state.v1.impl.reference.Ledger]]. There we
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
  *
  */
package object v1 {

  /** Identifier for the ledger, MUST match regexp [a-zA-Z0-9-]. */
  type LedgerId = Ref.SimpleString

  /** Identifiers for transactions.
    * Currently unrestricted unicode (See issue #398). */
  type TransactionId = String

  /** Identifiers used to correlate submission with results.
    * Currently unrestricted unicode (See issue #398). */
  type CommandId = String

  /** Identifiers used for correlating submission with a workflow.
    * Currently unrestricted unicode (See issue #398).  */
  type WorkflowId = String

  /** Identifiers for submitting client applications.
    * Currently unrestricted unicode (See issue #398). */
  type ApplicationId = String

  /** Identifiers for nodes in a transaction. */
  type NodeId = Transaction.NodeId

  /** Identifiers for packages. */
  type PackageId = Ref.PackageId

  /** Identifiers for parties. */
  type Party = Ref.Party

  /** Offsets into streams with hierarchical addressing.
    *
    * We use these [[Offset]]'s to address changes to the participant state.
    * We allow for array of [[Int]] to allow for hierarchical addresses.
    * These [[Int]] values are expected to be positive. Offsets are ordered by
    * lexicographic ordering of the array elements.
    *
    * A typical use case for [[Offset]]s would be addressing a transaction in a
    * blockchain by `[<blockheight>, <transactionId>]`. Depending on the
    * structure of the underlying ledger these offsets are more or less
    * nested, which is why we use an array of [[Int]]s. The expectation is
    * though that there usually are few elements in the array.
    *
    */
  final case class Offset(private val xs: Array[Long]) {
    override def toString: String =
      components.mkString("-")

    def components: Iterable[Long] = xs

    override def equals(that: Any): Boolean = that match {
      case o: Offset => Offset.compare(this, o) == 0
      case _ => false
    }
  }
  implicit object Offset extends Ordering[Offset] {

    /** Create an offset from a string of form 1-2-3. Throws
      * NumberFormatException on misformatted strings.
      */
    def assertFromString(s: String): Offset =
      Offset(s.split('-').map(_.toLong))

    override def compare(x: Offset, y: Offset): Int =
      scala.math.Ordering.Iterable[Long].compare(x.xs, y.xs)
  }

  /** The initial conditions of the ledger.
    *
    * @param ledgerId: The static ledger identifier.
    * @param initialRecordTime: The initial record time prior to any [[Update]] event.
    */
  final case class LedgerInitialConditions(
      ledgerId: LedgerId,
      initialRecordTime: Timestamp
  )

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
    GenTransaction[NodeId, Value.AbsoluteContractId, Value.VersionedValue[Value.AbsoluteContractId]]

  /** A contract instance with absolute contract identifiers only. */
  type AbsoluteContractInst =
    Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]]

  /** TODO (SM): expand this into a record for time-limit configuration and an
    *  explanation of how it is intended to be implemented.
    *  https://github.com/digital-asset/daml/issues/385
    */
  final case class Configuration(
      timeModel: TimeModel
  )

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
    *   [[WriteService.submitTransaction]] for how it relates to the notion of
    *   `recordTime`.
    *
    * @param workflowId: a submitter-provided identifier used for monitoring
    *   and to traffic-shape the work handled by DAML applications
    *   communicating over the ledger.
    *
    */
  final case class TransactionMeta(ledgerEffectiveTime: Timestamp, workflowId: WorkflowId)

}
