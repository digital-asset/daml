// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf.DamlLf

package object v1 {
  // FIXME(JM): Use the DAML-LF "SimpleString" where applicable?

  /** Identifiers for participant node states, MUST match regexp [a-zA-Z0-9-]. */
  type StateId = String

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

  /** Update identifier used to identify positions in the stream of updates. */
  type UpdateId = String

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

  /** Participant state update. */
  sealed trait Update {

    /** Short one-line description of what the state update is about. */
    def description: String
  }

  object Update {
    final case class StateInit(stateId: StateId) extends Update {
      override def description: String = s"Initialize with stateId=$stateId"
    }

    final case class Heartbeat(recordTime: Timestamp) extends Update {
      override def description: String = s"Heartbeat: $recordTime"
    }
    final case class ConfigurationChanged(newConfiguration: Configuration) extends Update {
      override def description: String = s"Configuration changed to: $newConfiguration"
    }
    final case class PartyAddedToParticipant(party: Party) extends Update {
      override def description: String = s"Add party '$party' to participant"
    }

    final case class PackageUploaded(
        optSubmitterInfo: Option[SubmitterInfo],
        archive: DamlLf.Archive)
        extends Update {
      override def description: String = s"Upload package ${archive.getHash}"
    }

    final case class TransactionAccepted(
        optSubmitterInfo: Option[SubmitterInfo],
        transactionMeta: TransactionMeta,
        transaction: CommittedTransaction,
        transactionId: String,
        recordTime: Timestamp,
        referencedContracts: List[(Value.AbsoluteContractId, AbsoluteContractInst)]
    ) extends Update {
      override def description: String = s"Accept transaction $transactionId"
    }

    final case class CommandRejected(
        optSubmitterInfo: Option[SubmitterInfo],
        reason: RejectionReason,
    ) extends Update {
      override def description: String = {
        val commandId = optSubmitterInfo.map(_.commandId).getOrElse("???")
        s"Reject command $commandId: $reason"
      }
    }
  }

  // Kinds of rejections
  sealed trait RejectionReason {
    def description: String
  }

  object RejectionReason {

    /** The transaction relied on contracts being active that were no longer
      * active at the point where it was sequenced.
      */
    final case object Inconsistent extends RejectionReason {
      override def description: String = "Inconsistent"
    }

    /** The Participant node did not have sufficient resources with the
      * to submit the transaction.
      */
    final case object ResourcesExhausted extends RejectionReason {
      override def description: String = "Resources exhausted"
    }

    /** The transaction submission timed out.
      *
      * This means the 'maximumRecordTime' was smaller than the recordTime seen
      * in an event in the Participant node.
      */
    final case object MaximumRecordTimeExceeded extends RejectionReason {
      override def description: String = "The maximum record time of the command exceeded"
    }

    /** The transaction submission was disputed.
      *
      * This means that the underlying ledger and its validation logic
      * considered the transaction potentially invalid. This can be due to a bug
      * in the submission or validiation logic, or due to malicious behaviour.
      */
    final case class Disputed(reason: String) extends RejectionReason {
      override def description: String = "Disputed: " + reason
    }

    /** The participant node has already seen a command with the same commandId
      * during its implementation specific deduplication window.
      *
      * TODO (SM): explain in more detail how command de-duplication should
      * work.
      */
    final case object DuplicateCommandId extends RejectionReason {
      override def description: String = "Duplicate command"
    }

    final case object PartyNotKnownOnLedger extends RejectionReason {
      override def description: String = "Party not known on ledger"
    }

    final case object SubmitterNotHostedOnParticipant extends RejectionReason {
      override def description: String = "Submitter not hosted on participant"
    }
  }

  trait ReadService {
    def stateUpdates(beginAfter: Option[UpdateId]): Source[(UpdateId, Update), NotUsed]
  }

  trait WriteService {
    def submitTransaction(
        stateId: StateId,
        submitterInfo: SubmitterInfo,
        transactionMeta: TransactionMeta,
        transaction: SubmittedTransaction): Unit
  }

  object WriteService {
    sealed trait Err
    object Err {
      // TODO: consider other errors
      final case class StateIdMismatch(expected: StateId, actual: StateId) extends Err
    }
  }
}
