// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.error.GrpcStatuses
import com.daml.logging.entries.{LoggingEntry, LoggingValue, ToLoggingValue}
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.{RequestCounter, data}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.{BlindingInfo, CommittedTransaction}
import com.digitalasset.daml.lf.value.Value
import com.google.rpc.status.Status as RpcStatus

import scala.concurrent.Promise

/** An update to the (abstract) participant state.
  *
  * [[Update]]'s are used in to communicate
  * changes to abstract participant state to consumers.
  *
  * We describe the possible updates in the comments of
  * each of the case classes implementing [[Update]].
  *
  * Deduplication guarantee:
  * Let there be a [[Update.TransactionAccepted]] with [[CompletionInfo]]
  * or a [[Update.CommandRejected]] with [[CompletionInfo]] at offset `off2`.
  * If `off2`'s [[CompletionInfo.optDeduplicationPeriod]] is a [[com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationOffset]],
  * let `off1` be the first offset after the deduplication offset.
  * If the deduplication period is a [[com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationDuration]],
  * let `off1` be the first offset whose record time is at most the duration before `off2`'s record time (inclusive).
  * Then there is no other [[Update.TransactionAccepted]] with [[CompletionInfo]] for the same [[CompletionInfo.changeId]]
  * between the offsets `off1` and `off2` inclusive.
  *
  * So if a command submission has resulted in a [[Update.TransactionAccepted]],
  * other command submissions with the same [[SubmitterInfo.changeId]] must be deduplicated
  * if the earlier's [[Update.TransactionAccepted]] falls within the latter's [[CompletionInfo.optDeduplicationPeriod]].
  *
  * Implementations MAY extend the deduplication period from [[SubmitterInfo]] arbitrarily
  * and reject a command submission as a duplicate even if its deduplication period does not include
  * the earlier's [[Update.TransactionAccepted]].
  * A [[Update.CommandRejected]] completion does not trigger deduplication and implementations SHOULD
  * process such resubmissions normally.
  */
sealed trait Update extends Product with Serializable with PrettyPrinting {

  /** The record time at which the state change was committed. */
  def recordTime: Timestamp

  def domainIndexOpt: Option[(DomainId, DomainIndex)]

  // TODO(i20043) this will be moved later to a wrapper object to maintain equality semantics
  def persisted: Promise[Unit]

  def withRecordTime(recordTime: Timestamp): Update
}

sealed trait WithoutDomainIndex extends Update {
  override def domainIndexOpt: Option[(DomainId, DomainIndex)] = None
}

object Update {

  /** Produces a constant dummy transaction seed for transactions in which we cannot expose a seed. Essentially all of
    * them. TransactionMeta.submissionSeed can no longer be set to None starting with Daml 1.3
    */
  def noOpSeed: LfHash =
    LfHash.assertFromString("00" * LfHash.underlyingHashLength)

  /** Signal used only to increase the offset of the participant in the initialization stage. */
  final case class Init(
      recordTime: Timestamp,
      persisted: Promise[Unit] = Promise(),
  ) extends Update
      with WithoutDomainIndex {

    override protected def pretty: Pretty[Init] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        indicateOmittedFields,
      )

    override def withRecordTime(recordTime: Timestamp): Update = this.copy(recordTime = recordTime)

  }

  object Init {
    implicit val `Init to LoggingValue`: ToLoggingValue[Init] = { case Init(recordTime, _) =>
      LoggingValue.Nested.fromEntries(
        Logging.recordTime(recordTime)
      )
    }
  }

  /** Signal that a party is hosted at a participant.
    *
    * Repeated `PartyAddedToParticipant` updates are interpreted in the order of their offsets as follows:
    * - last-write-wins semantics for `displayName`
    * - set-union semantics for `participantId`; i.e., parties can only be added to, but not removed from a participant
    * The `recordTime` and `submissionId` are always metadata for their specific `PartyAddedToParticipant` update.
    *
    * @param party         The party identifier.
    * @param displayName   The user readable description of the party. May not be unique.
    * @param participantId The participant that this party was added to.
    * @param recordTime    The ledger-provided timestamp at which the party was allocated.
    * @param submissionId  The submissionId of the command which requested party to be added.
    */
  final case class PartyAddedToParticipant(
      party: Ref.Party,
      displayName: String,
      participantId: Ref.ParticipantId,
      recordTime: Timestamp,
      submissionId: Option[Ref.SubmissionId],
      persisted: Promise[Unit] = Promise(),
  ) extends Update
      with WithoutDomainIndex {
    override protected def pretty: Pretty[PartyAddedToParticipant] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("party", _.party),
        param("displayName", _.displayName.singleQuoted),
        param("participantId", _.participantId),
        indicateOmittedFields,
      )

    override def withRecordTime(recordTime: Timestamp): Update = this.copy(recordTime = recordTime)
  }

  object PartyAddedToParticipant {
    implicit val `PartyAddedToParticipant to LoggingValue`
        : ToLoggingValue[PartyAddedToParticipant] = {
      case PartyAddedToParticipant(
            party,
            displayName,
            participantId,
            recordTime,
            submissionId,
            _,
          ) =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(recordTime),
          Logging.submissionIdOpt(submissionId),
          Logging.participantId(participantId),
          Logging.party(party),
          Logging.displayName(displayName),
        )
    }
  }

  /** Signal that the party allocation request has been Rejected.
    *
    * @param submissionId    submissionId of the party allocation command.
    * @param participantId   The participant to which the party was requested to be added. This
    *                        field is informative.
    * @param recordTime      The ledger-provided timestamp at which the party was added.
    * @param rejectionReason Reason for rejection of the party allocation entry.
    */
  final case class PartyAllocationRejected(
      submissionId: Ref.SubmissionId,
      participantId: Ref.ParticipantId,
      recordTime: Timestamp,
      rejectionReason: String,
      persisted: Promise[Unit] = Promise(),
  ) extends Update
      with WithoutDomainIndex {
    override protected def pretty: Pretty[PartyAllocationRejected] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("participantId", _.participantId),
        param("rejectionReason", _.rejectionReason.singleQuoted),
      )

    override def withRecordTime(recordTime: Timestamp): Update = this.copy(recordTime = recordTime)
  }

  object PartyAllocationRejected {
    implicit val `PartyAllocationRejected to LoggingValue`
        : ToLoggingValue[PartyAllocationRejected] = {
      case PartyAllocationRejected(submissionId, participantId, recordTime, rejectionReason, _) =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(recordTime),
          Logging.submissionId(submissionId),
          Logging.participantId(participantId),
          Logging.rejectionReason(rejectionReason),
        )
    }
  }

  /** Signal the acceptance of a transaction.
    *
    * @param completionInfoO The information provided by the submitter of the command that
    *                          created this transaction. It must be provided if this participant
    *                          hosts one of the [[SubmitterInfo.actAs]] parties and shall output a
    *                          completion event for this transaction. This in particular applies if
    *                          this participant has submitted the command to the [[WriteService]].
    *
    *                          The Offset-order of Updates must ensure that command
    *                          deduplication guarantees are met.
    * @param transactionMeta   The metadata of the transaction that was provided by the submitter.
    *                          It is visible to all parties that can see the transaction.
    * @param transaction       The view of the transaction that was accepted. This view must
    *                          include at least the projection of the accepted transaction to the
    *                          set of all parties hosted at this participant. See
    *                          https://docs.daml.com/concepts/ledger-model/ledger-privacy.html
    *                          on how these views are computed.
    *
    *                          Note that ledgers with weaker privacy models can decide to forgo
    *                          projections of transactions and always show the complete
    *                          transaction.
    * @param recordTime        The ledger-provided timestamp at which the transaction was recorded.
    *                          The last [[com.digitalasset.canton.ledger.configuration.Configuration]] set before this [[TransactionAccepted]]
    *                          determines how this transaction's recordTime relates to its
    *                          [[TransactionMeta.ledgerEffectiveTime]].
    * @param contractMetadata  For each contract created in this transaction, this map may contain
    *                          contract metadata assigned by the ledger implementation.
    *                          This data is opaque and can only be used in [[com.digitalasset.daml.lf.transaction.FatContractInstance]]s
    *                          when submitting transactions trough the [[WriteService]].
    *                          If a contract created by this transaction is not element of this map,
    *                          its metadata is equal to the empty byte array.
    */
  final case class TransactionAccepted(
      completionInfoO: Option[CompletionInfo],
      transactionMeta: TransactionMeta,
      transaction: CommittedTransaction,
      updateId: data.UpdateId,
      recordTime: Timestamp,
      hostedWitnesses: List[Ref.Party],
      contractMetadata: Map[Value.ContractId, Bytes],
      domainId: DomainId,
      domainIndex: Option[
        DomainIndex
      ], // TODO(i20043) this will be simplified as Update refactoring is unconstrained by serialization
      persisted: Promise[Unit] = Promise(),
  ) extends Update {
    // TODO(i20043) this will be simplified as Update refactoring is unconstrained by serialization
    assert(completionInfoO.forall(_.messageUuid.isEmpty))
    assert(domainIndex.exists(_.requestIndex.isDefined))
    val blindingInfo: BlindingInfo = Blinding.blind(transaction)

    override protected def pretty: Pretty[TransactionAccepted] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("updateId", _.updateId),
        param("transactionMeta", _.transactionMeta),
        paramIfDefined("completion", _.completionInfoO),
        param("nodes", _.transaction.nodes.size),
        param("roots", _.transaction.roots.length),
        indicateOmittedFields,
      )

    override def domainIndexOpt: Option[(DomainId, DomainIndex)] = domainIndex.map(
      domainId -> _
    )

    override def withRecordTime(recordTime: Timestamp): Update = throw new IllegalStateException(
      "Record time is not supposed to be overridden for sequenced events"
    )
  }

  object TransactionAccepted {
    implicit val `TransactionAccepted to LoggingValue`: ToLoggingValue[TransactionAccepted] = {
      case TransactionAccepted(
            completionInfoO,
            transactionMeta,
            _,
            updateId,
            recordTime,
            _,
            _,
            domainId,
            _,
            _,
          ) =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(recordTime),
          Logging.completionInfo(completionInfoO),
          Logging.updateId(updateId),
          Logging.ledgerTime(transactionMeta.ledgerEffectiveTime),
          Logging.workflowIdOpt(transactionMeta.workflowId),
          Logging.submissionTime(transactionMeta.submissionTime),
          Logging.domainId(domainId),
        )
    }
  }

  /** @param optCompletionInfo The information provided by the submitter of the command that
    *                          created this reassignment. It must be provided if this participant
    *                          hosts the submitter and shall output a completion event for this
    *                          reassignment. This in particular applies if this participant has
    *                          submitted the command to the [[WriteService]].
    * @param workflowId        a submitter-provided identifier used for monitoring
    *                          and to traffic-shape the work handled by Daml applications
    *                          communicating over the ledger.
    * @param updateId          A unique identifier for this update assigned by the ledger.
    * @param recordTime        The ledger-provided timestamp at which the reassignment was recorded.
    * @param reassignmentInfo  Common part of all type of reassignments.
    */
  final case class ReassignmentAccepted(
      optCompletionInfo: Option[CompletionInfo],
      workflowId: Option[Ref.WorkflowId],
      updateId: data.UpdateId,
      recordTime: Timestamp,
      reassignmentInfo: ReassignmentInfo,
      reassignment: Reassignment,
      domainIndex: Option[
        DomainIndex
      ], // TODO(i20043) this will be simplified as Update refactoring is unconstrained by serialization
      persisted: Promise[Unit] = Promise(),
  ) extends Update {
    // TODO(i20043) this will be simplified as Update refactoring is unconstrained by serialization
    assert(optCompletionInfo.forall(_.messageUuid.isEmpty))
    assert(domainIndex.exists(_.requestIndex.isDefined))

    override protected def pretty: Pretty[ReassignmentAccepted] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("updateId", _.updateId),
        paramIfDefined("completion", _.optCompletionInfo),
        param("source", _.reassignmentInfo.sourceDomain),
        param("target", _.reassignmentInfo.targetDomain),
        unnamedParam(_.reassignment.kind.unquoted),
        indicateOmittedFields,
      )

    def domainId: DomainId = reassignment match {
      case _: Reassignment.Assign => reassignmentInfo.targetDomain.unwrap
      case _: Reassignment.Unassign => reassignmentInfo.sourceDomain.unwrap
    }

    override def domainIndexOpt: Option[(DomainId, DomainIndex)] = domainIndex.map(
      domainId -> _
    )

    override def withRecordTime(recordTime: Timestamp): Update = throw new IllegalStateException(
      "Record time is not supposed to be overridden for sequenced events"
    )
  }

  object ReassignmentAccepted {
    implicit val `ReassignmentAccepted to LoggingValue`: ToLoggingValue[ReassignmentAccepted] = {
      case ReassignmentAccepted(
            optCompletionInfo,
            workflowId,
            updateId,
            recordTime,
            _,
            _,
            _,
            _,
          ) =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(recordTime),
          Logging.completionInfo(optCompletionInfo),
          Logging.updateId(updateId),
          Logging.workflowIdOpt(workflowId),
        )
    }
  }

  /** Signal that a command submitted via [[WriteService]] was rejected.
    *
    * @param recordTime     The record time of the completion
    * @param completionInfo The completion information for the submission
    * @param reasonTemplate A template for generating the gRPC status code with error details.
    *                       See ``error.proto`` for the status codes of common rejection reasons.
    */
  final case class CommandRejected(
      recordTime: Timestamp,
      completionInfo: CompletionInfo,
      reasonTemplate: CommandRejected.RejectionReasonTemplate,
      domainId: DomainId,
      domainIndex: Option[DomainIndex],
      persisted: Promise[Unit] = Promise(),
  ) extends Update {
    // TODO(i20043) this will be simplified as Update refactoring is unconstrained by serialization
    assert(
      // rejection from sequencer should have the request sequencer counter
      (completionInfo.messageUuid.isEmpty && domainIndex.exists(
        _.requestIndex.exists(_.sequencerCounter.isDefined)
      ))
      // rejection from participant (timeout, unsequenced) should have the messageUuid, and no domainIndex
        || (completionInfo.messageUuid.isDefined && domainIndex.isEmpty)
    )

    override protected def pretty: Pretty[CommandRejected] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("completion", _.completionInfo),
        paramIfTrue("definiteAnswer", _.definiteAnswer),
        param("reason", _.reasonTemplate.message.singleQuoted),
        param("domainId", _.domainId.uid),
      )

    /** If true, the deduplication guarantees apply to this rejection.
      * The participant state implementations should strive to set this flag to true as often as
      * possible so that applications get better guarantees.
      */
    def definiteAnswer: Boolean = reasonTemplate.definiteAnswer

    override def domainIndexOpt: Option[(DomainId, DomainIndex)] = domainIndex.map(domainId -> _)

    override def withRecordTime(recordTime: Timestamp): Update =
      if (domainIndex.nonEmpty)
        throw new IllegalStateException(
          "Record time is not supposed to be overridden for sequenced events"
        )
      else this.copy(recordTime = recordTime)
  }

  object CommandRejected {

    implicit val `CommandRejected to LoggingValue`: ToLoggingValue[CommandRejected] = {
      case CommandRejected(recordTime, submitterInfo, reason, domainId, _, _) =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(recordTime),
          Logging.submitter(submitterInfo.actAs),
          Logging.applicationId(submitterInfo.applicationId),
          Logging.commandId(submitterInfo.commandId),
          Logging.deduplicationPeriod(submitterInfo.optDeduplicationPeriod),
          Logging.rejectionReason(reason),
          Logging.domainId(domainId),
        )
    }

    /** A template for generating gRPC status codes.
      */
    sealed trait RejectionReasonTemplate {

      /** A human-readable description of the error */
      def message: String

      /** A gRPC status code representing the error. */
      def code: Int

      /** A protobuf gRPC status representing the error. */
      def status: RpcStatus

      /** Whether the rejection is a definite answer for the deduplication guarantees
        * specified for [[Update]].
        */
      def definiteAnswer: Boolean
    }

    object RejectionReasonTemplate {
      implicit val `RejectionReasonTemplate to LoggingValue`
          : ToLoggingValue[RejectionReasonTemplate] =
        reason =>
          LoggingValue.Nested.fromEntries(
            "code" -> reason.code,
            "message" -> reason.message,
            "definiteAnswer" -> reason.definiteAnswer,
          )
    }

    /** The status code for the command rejection. */
    final case class FinalReason(override val status: RpcStatus) extends RejectionReasonTemplate {
      override def message: String = status.message

      override def code: Int = status.code

      override def definiteAnswer: Boolean = GrpcStatuses.isDefiniteAnswer(status)
    }
  }

  final case class SequencerIndexMoved(
      domainId: DomainId,
      sequencerIndex: SequencerIndex,
      requestCounterO: Option[RequestCounter],
      persisted: Promise[Unit] = Promise(),
  ) extends Update {
    override protected def pretty: Pretty[SequencerIndexMoved] =
      prettyOfClass(
        param("domainId", _.domainId.uid),
        param("sequencerCounter", _.sequencerIndex.counter),
        param("sequencerTimestamp", _.sequencerIndex.timestamp),
        paramIfDefined("requestCounter", _.requestCounterO),
      )

    override def domainIndexOpt: Option[(DomainId, DomainIndex)] = Some(
      domainId -> DomainIndex(
        requestIndex = requestCounterO.map(requestCounter =>
          RequestIndex(
            counter = requestCounter,
            sequencerCounter = Some(sequencerIndex.counter),
            timestamp = sequencerIndex.timestamp,
          )
        ),
        sequencerIndex = Some(sequencerIndex),
      )
    )

    override def recordTime: Timestamp = sequencerIndex.timestamp.underlying

    override def withRecordTime(recordTime: Timestamp): Update = throw new IllegalStateException(
      "Record time is not supposed to be overridden for sequenced events"
    )
  }

  object SequencerIndexMoved {
    implicit val `SequencerIndexMoved to LoggingValue`: ToLoggingValue[SequencerIndexMoved] =
      seqIndexMoved =>
        LoggingValue.Nested.fromEntries(
          Logging.domainId(seqIndexMoved.domainId),
          "sequencerCounter" -> seqIndexMoved.sequencerIndex.counter.unwrap,
          "sequencerTimestamp" -> seqIndexMoved.sequencerIndex.timestamp.underlying.toInstant,
        )
  }

  final case class CommitRepair() extends Update {
    override val persisted: Promise[Unit] = Promise()

    override val domainIndexOpt: Option[(DomainId, DomainIndex)] = None

    override protected def pretty: Pretty[CommitRepair] = prettyOfClass()

    override def withRecordTime(recordTime: Timestamp): Update = throw new IllegalStateException(
      "Record time is not supposed to be overridden for CommitRepair events"
    )

    override val recordTime: Timestamp = Timestamp.now()
  }

  implicit val `Update to LoggingValue`: ToLoggingValue[Update] = {
    case update: Init =>
      Init.`Init to LoggingValue`.toLoggingValue(update)
    case update: PartyAddedToParticipant =>
      PartyAddedToParticipant.`PartyAddedToParticipant to LoggingValue`.toLoggingValue(update)
    case update: PartyAllocationRejected =>
      PartyAllocationRejected.`PartyAllocationRejected to LoggingValue`.toLoggingValue(update)
    case update: TransactionAccepted =>
      TransactionAccepted.`TransactionAccepted to LoggingValue`.toLoggingValue(update)
    case update: CommandRejected =>
      CommandRejected.`CommandRejected to LoggingValue`.toLoggingValue(update)
    case update: ReassignmentAccepted =>
      ReassignmentAccepted.`ReassignmentAccepted to LoggingValue`.toLoggingValue(update)
    case update: SequencerIndexMoved =>
      SequencerIndexMoved.`SequencerIndexMoved to LoggingValue`.toLoggingValue(update)
    case _: CommitRepair =>
      LoggingValue.Empty
  }

  private object Logging {
    def recordTime(timestamp: Timestamp): LoggingEntry =
      "recordTime" -> timestamp.toInstant

    def submissionId(id: Ref.SubmissionId): LoggingEntry =
      "submissionId" -> id

    def submissionIdOpt(id: Option[Ref.SubmissionId]): LoggingEntry =
      "submissionId" -> id

    def participantId(id: Ref.ParticipantId): LoggingEntry =
      "participantId" -> id

    def commandId(id: Ref.CommandId): LoggingEntry =
      "commandId" -> id

    def party(party: Ref.Party): LoggingEntry =
      "party" -> party

    def updateId(id: data.UpdateId): LoggingEntry =
      "updateId" -> id

    def applicationId(id: Ref.ApplicationId): LoggingEntry =
      "applicationId" -> id

    def workflowIdOpt(id: Option[Ref.WorkflowId]): LoggingEntry =
      "workflowId" -> id

    def ledgerTime(time: Timestamp): LoggingEntry =
      "ledgerTime" -> time.toInstant

    def submissionTime(time: Timestamp): LoggingEntry =
      "submissionTime" -> time.toInstant

    def deduplicationPeriod(period: Option[DeduplicationPeriod]): LoggingEntry =
      "deduplicationPeriod" -> period

    def rejectionReason(rejectionReason: String): LoggingEntry =
      "rejectionReason" -> rejectionReason

    def rejectionReason(
        rejectionReasonTemplate: CommandRejected.RejectionReasonTemplate
    ): LoggingEntry =
      "rejectionReason" -> rejectionReasonTemplate

    def displayName(name: String): LoggingEntry =
      "displayName" -> name

    def submitter(parties: List[Ref.Party]): LoggingEntry =
      "submitter" -> parties

    def completionInfo(info: Option[CompletionInfo]): LoggingEntry =
      "completion" -> info

    def domainId(domainId: DomainId): LoggingEntry =
      "domainId" -> domainId.toString
  }

}
