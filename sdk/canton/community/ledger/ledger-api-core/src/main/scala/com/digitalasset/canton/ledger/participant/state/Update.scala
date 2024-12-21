// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.error.GrpcStatuses
import com.daml.logging.entries.{LoggingEntry, LoggingValue, ToLoggingValue}
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod}
import com.digitalasset.canton.ledger.participant.state.Update.CommandRejected.RejectionReasonTemplate
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{HasTraceContext, TraceContext}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.{RequestCounter, SequencerCounter, data}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.{BlindingInfo, CommittedTransaction}
import com.digitalasset.daml.lf.value.Value
import com.google.rpc.status.Status as RpcStatus

import java.util.UUID
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
sealed trait Update extends Product with Serializable with PrettyPrinting with HasTraceContext {

  /** The record time at which the state change was committed. */
  def recordTime: CantonTimestamp
}

// TODO(i21341) this will be removed later as Topology Event project progresses
sealed trait ParticipantUpdate extends Update {
  def withRecordTime(recordTime: CantonTimestamp): Update

  def persisted: Promise[Unit]
}

sealed trait DomainUpdate extends Update {
  def domainId: DomainId
}

/** Update which defines a DomainIndex, and therefore contribute to DomainIndex moving ahead.
  */
sealed trait DomainIndexUpdate extends DomainUpdate {
  def requestCounterO: Option[RequestCounter]

  def sequencerCounterO: Option[SequencerCounter]

  final def requestIndexO: Option[RequestIndex] =
    requestCounterO.map(RequestIndex(_, sequencerCounterO, recordTime))

  final def sequencerIndexO: Option[SequencerIndex] =
    sequencerCounterO.map(SequencerIndex(_, recordTime))

  final def domainIndex: (DomainId, DomainIndex) =
    domainId -> DomainIndex(requestIndexO, sequencerIndexO, recordTime)
}

sealed trait SequencedUpdate extends DomainIndexUpdate {
  def sequencerCounter: SequencerCounter

  final override def sequencerCounterO: Option[SequencerCounter] = Some(sequencerCounter)
}

sealed trait RequestUpdate extends DomainIndexUpdate {
  def requestCounter: RequestCounter

  final override def requestCounterO: Option[RequestCounter] = Some(requestCounter)
}

sealed trait FloatingUpdate extends DomainIndexUpdate {

  final override def requestCounterO: Option[RequestCounter] = None

  final override def sequencerCounterO: Option[SequencerCounter] = None
}

sealed trait RepairUpdate extends RequestUpdate {
  final override def sequencerCounterO: Option[SequencerCounter] = None
}

trait LapiCommitSet

sealed trait CommitSetUpdate extends RequestUpdate with SequencedUpdate {
  protected def commitSetO: Option[LapiCommitSet]

  /** Expected to be set already when accessed
    * @return IllegalStateException if not set
    */
  def commitSet(implicit errorLoggingContext: ErrorLoggingContext): LapiCommitSet =
    commitSetO.getOrElse(
      ErrorUtil.invalidState("CommitSet not specified.")
    )

  def withCommitSet(commitSet: LapiCommitSet): CommitSetUpdate
}

object Update {

  /** Produces a constant dummy transaction seed for transactions in which we cannot expose a seed. Essentially all of
    * them. TransactionMeta.submissionSeed can no longer be set to None starting with Daml 1.3
    */
  def noOpSeed: LfHash =
    LfHash.assertFromString("00" * LfHash.underlyingHashLength)

  /** Signal that a party is hosted at a participant.
    *
    * Repeated `PartyAddedToParticipant` updates are interpreted in the order of their offsets as follows:
    * - set-union semantics for `participantId`; i.e., parties can only be added to, but not removed from a participant
    * The `recordTime` and `submissionId` are always metadata for their specific `PartyAddedToParticipant` update.
    *
    * @param party         The party identifier.
    * @param participantId The participant that this party was added to.
    * @param recordTime    The ledger-provided timestamp at which the party was allocated.
    * @param submissionId  The submissionId of the command which requested party to be added.
    */
  final case class PartyAddedToParticipant(
      party: Ref.Party,
      participantId: Ref.ParticipantId,
      recordTime: CantonTimestamp,
      submissionId: Option[Ref.SubmissionId],
      persisted: Promise[Unit] = Promise(),
  )(implicit override val traceContext: TraceContext)
      extends ParticipantUpdate {
    override protected def pretty: Pretty[PartyAddedToParticipant] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("party", _.party),
        param("participantId", _.participantId),
        indicateOmittedFields,
      )

    override def withRecordTime(recordTime: CantonTimestamp): Update =
      this.copy(recordTime = recordTime)
  }

  object PartyAddedToParticipant {
    implicit val `PartyAddedToParticipant to LoggingValue`
        : ToLoggingValue[PartyAddedToParticipant] = {
      case PartyAddedToParticipant(
            party,
            participantId,
            recordTime,
            submissionId,
            _,
          ) =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(recordTime.toLf),
          Logging.submissionIdOpt(submissionId),
          Logging.participantId(participantId),
          Logging.party(party),
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
      recordTime: CantonTimestamp,
      rejectionReason: String,
      persisted: Promise[Unit] = Promise(),
  )(implicit override val traceContext: TraceContext)
      extends ParticipantUpdate {
    override protected def pretty: Pretty[PartyAllocationRejected] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("participantId", _.participantId),
        param("rejectionReason", _.rejectionReason.singleQuoted),
      )

    override def withRecordTime(recordTime: CantonTimestamp): Update =
      this.copy(recordTime = recordTime)
  }

  object PartyAllocationRejected {
    implicit val `PartyAllocationRejected to LoggingValue`
        : ToLoggingValue[PartyAllocationRejected] = {
      case PartyAllocationRejected(submissionId, participantId, recordTime, rejectionReason, _) =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(recordTime.toLf),
          Logging.submissionId(submissionId),
          Logging.participantId(participantId),
          Logging.rejectionReason(rejectionReason),
        )
    }
  }

  final case class TopologyTransactionEffective(
      updateId: Ref.TransactionId,
      events: Set[TopologyTransactionEffective.TopologyEvent],
      domainId: DomainId,
      effectiveTime: CantonTimestamp,
  )(implicit override val traceContext: TraceContext)
      extends FloatingUpdate {

    // Topology transactions emitted to the update stream at effective time
    override def recordTime: CantonTimestamp = effectiveTime

    override def pretty: Pretty[TopologyTransactionEffective] =
      prettyOfClass(
        param("effectiveTime", _.effectiveTime),
        param("domainId", _.domainId),
        param("updateId", _.updateId),
        indicateOmittedFields,
      )
  }

  object TopologyTransactionEffective {

    sealed trait AuthorizationLevel
    object AuthorizationLevel {
      final case object Submission extends AuthorizationLevel

      final case object Confirmation extends AuthorizationLevel

      final case object Observation extends AuthorizationLevel
      final case object Revoked extends AuthorizationLevel
    }
    sealed trait TopologyEvent

    object TopologyEvent {
      final case class PartyToParticipantAuthorization(
          party: Ref.Party,
          participant: Ref.ParticipantId,
          level: AuthorizationLevel,
      ) extends TopologyEvent
    }
    implicit val `TopologyTransactionEffective to LoggingValue`
        : ToLoggingValue[TopologyTransactionEffective] = { topologyTransactionEffective =>
      LoggingValue.Nested.fromEntries(
        Logging.updateId(topologyTransactionEffective.updateId),
        Logging.recordTime(topologyTransactionEffective.recordTime.toLf),
        Logging.domainId(topologyTransactionEffective.domainId),
      )
    }
  }

  /** Signal the acceptance of a transaction.
    */
  trait TransactionAccepted extends RequestUpdate {

    /** The information provided by the submitter of the command that
      * created this transaction. It must be provided if this participant
      * hosts one of the [[SubmitterInfo.actAs]] parties and shall output a
      * completion event for this transaction. This in particular applies if
      * this participant has submitted the command to the [[SyncService]].
      *
      * The Offset-order of Updates must ensure that command
      * deduplication guarantees are met.
      */
    def completionInfoO: Option[CompletionInfo]

    /** The metadata of the transaction that was provided by the submitter.
      * It is visible to all parties that can see the transaction.
      */
    def transactionMeta: TransactionMeta

    /** The view of the transaction that was accepted. This view must
      * include at least the projection of the accepted transaction to the
      * set of all parties hosted at this participant. See
      * https://docs.daml.com/concepts/ledger-model/ledger-privacy.html
      * on how these views are computed.
      *
      * Note that ledgers with weaker privacy models can decide to forgo
      * projections of transactions and always show the complete
      * transaction.
      */
    def transaction: CommittedTransaction

    def updateId: data.UpdateId

    def hostedWitnesses: List[Ref.Party]

    /** For each contract created in this transaction, this map may contain
      * contract metadata assigned by the ledger implementation.
      * This data is opaque and can only be used in [[com.digitalasset.daml.lf.transaction.FatContractInstance]]s
      * when submitting transactions trough the [[SyncService]].
      * If a contract created by this transaction is not element of this map,
      * its metadata is equal to the empty byte array.
      */
    def contractMetadata: Map[Value.ContractId, Bytes]

    lazy val blindingInfo: BlindingInfo = Blinding.blind(transaction)

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
  }

  object TransactionAccepted {
    implicit val `TransactionAccepted to LoggingValue`: ToLoggingValue[TransactionAccepted] = {
      case txAccepted: TransactionAccepted =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(txAccepted.recordTime.toLf),
          Logging.completionInfo(txAccepted.completionInfoO),
          Logging.updateId(txAccepted.updateId),
          Logging.ledgerTime(txAccepted.transactionMeta.ledgerEffectiveTime),
          Logging.workflowIdOpt(txAccepted.transactionMeta.workflowId),
          Logging.submissionTime(txAccepted.transactionMeta.submissionTime),
          Logging.domainId(txAccepted.domainId),
        )
    }
  }

  final case class SequencedTransactionAccepted(
      completionInfoO: Option[CompletionInfo],
      transactionMeta: TransactionMeta,
      transaction: CommittedTransaction,
      updateId: data.UpdateId,
      hostedWitnesses: List[Ref.Party],
      contractMetadata: Map[Value.ContractId, Bytes],
      domainId: DomainId,
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      recordTime: CantonTimestamp,
      commitSetO: Option[LapiCommitSet] = None,
  )(implicit override val traceContext: TraceContext)
      extends TransactionAccepted
      with SequencedUpdate
      with CommitSetUpdate {
    override def withCommitSet(commitSet: LapiCommitSet): CommitSetUpdate =
      this.copy(commitSetO = Some(commitSet))
  }

  final case class RepairTransactionAccepted(
      transactionMeta: TransactionMeta,
      transaction: CommittedTransaction,
      updateId: data.UpdateId,
      hostedWitnesses: List[Ref.Party],
      contractMetadata: Map[Value.ContractId, Bytes],
      domainId: DomainId,
      requestCounter: RequestCounter,
      recordTime: CantonTimestamp,
  )(implicit override val traceContext: TraceContext)
      extends TransactionAccepted
      with RepairUpdate {

    override def completionInfoO: Option[CompletionInfo] = None
  }

  trait ReassignmentAccepted extends RequestUpdate {

    /** The information provided by the submitter of the command that
      * created this reassignment. It must be provided if this participant
      * hosts the submitter and shall output a completion event for this
      * reassignment. This in particular applies if this participant has
      * submitted the command to the [[SyncService]].
      */
    def optCompletionInfo: Option[CompletionInfo]

    /** A submitter-provided identifier used for monitoring
      * and to traffic-shape the work handled by Daml applications
      */
    def workflowId: Option[Ref.WorkflowId]

    /** A unique identifier for this update assigned by the ledger.
      */
    def updateId: data.UpdateId

    /** Common part of all type of reassignments.
      */
    def reassignmentInfo: ReassignmentInfo

    def reassignment: Reassignment

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

    final override def domainId: DomainId = reassignment match {
      case _: Reassignment.Assign => reassignmentInfo.targetDomain.unwrap
      case _: Reassignment.Unassign => reassignmentInfo.sourceDomain.unwrap
    }
  }

  final case class SequencedReassignmentAccepted(
      optCompletionInfo: Option[CompletionInfo],
      workflowId: Option[Ref.WorkflowId],
      updateId: data.UpdateId,
      reassignmentInfo: ReassignmentInfo,
      reassignment: Reassignment,
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      recordTime: CantonTimestamp,
      commitSetO: Option[LapiCommitSet] = None,
  )(implicit override val traceContext: TraceContext)
      extends ReassignmentAccepted
      with SequencedUpdate
      with CommitSetUpdate {
    override def withCommitSet(commitSet: LapiCommitSet): CommitSetUpdate =
      this.copy(commitSetO = Some(commitSet))
  }

  final case class RepairReassignmentAccepted(
      workflowId: Option[Ref.WorkflowId],
      updateId: data.UpdateId,
      reassignmentInfo: ReassignmentInfo,
      reassignment: Reassignment,
      requestCounter: RequestCounter,
      recordTime: CantonTimestamp,
  )(implicit override val traceContext: TraceContext)
      extends ReassignmentAccepted
      with RepairUpdate {
    override def optCompletionInfo: Option[CompletionInfo] = None
  }

  object ReassignmentAccepted {
    implicit val `ReassignmentAccepted to LoggingValue`: ToLoggingValue[ReassignmentAccepted] = {
      case reassignmentAccepted: ReassignmentAccepted =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(reassignmentAccepted.recordTime.toLf),
          Logging.completionInfo(reassignmentAccepted.optCompletionInfo),
          Logging.updateId(reassignmentAccepted.updateId),
          Logging.workflowIdOpt(reassignmentAccepted.workflowId),
        )
    }
  }

  /** Signal that a command submitted via [[SyncService]] was rejected.
    */
  sealed trait CommandRejected extends DomainIndexUpdate {

    /** The completion information for the submission
      */
    def completionInfo: CompletionInfo

    /** A template for generating the gRPC status code with error details.
      * See ``error.proto`` for the status codes of common rejection reasons.
      */
    def reasonTemplate: RejectionReasonTemplate

    /** If true, the deduplication guarantees apply to this rejection.
      * The participant state implementations should strive to set this flag to true as often as
      * possible so that applications get better guarantees.
      */
    final def definiteAnswer: Boolean = reasonTemplate.definiteAnswer

    override protected def pretty: Pretty[CommandRejected] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("completion", _.completionInfo),
        paramIfTrue("definiteAnswer", _.definiteAnswer),
        param("reason", _.reasonTemplate.message.singleQuoted),
        param("domainId", _.domainId.uid),
      )
  }

  final case class SequencedCommandRejected(
      completionInfo: CompletionInfo,
      reasonTemplate: RejectionReasonTemplate,
      domainId: DomainId,
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      recordTime: CantonTimestamp,
  )(implicit override val traceContext: TraceContext)
      extends CommandRejected
      with SequencedUpdate
      with RequestUpdate

  final case class UnSequencedCommandRejected(
      completionInfo: CompletionInfo,
      reasonTemplate: RejectionReasonTemplate,
      domainId: DomainId,
      recordTime: CantonTimestamp,
      messageUuid: UUID,
  )(implicit override val traceContext: TraceContext)
      extends CommandRejected
      with FloatingUpdate

  object CommandRejected {

    implicit val `CommandRejected to LoggingValue`: ToLoggingValue[CommandRejected] = {
      case commandRejected: CommandRejected =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(commandRejected.recordTime.toLf),
          Logging.submitter(commandRejected.completionInfo.actAs),
          Logging.applicationId(commandRejected.completionInfo.applicationId),
          Logging.commandId(commandRejected.completionInfo.commandId),
          Logging.deduplicationPeriod(commandRejected.completionInfo.optDeduplicationPeriod),
          Logging.rejectionReason(commandRejected.reasonTemplate),
          Logging.domainId(commandRejected.domainId),
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
      sequencerCounter: SequencerCounter,
      recordTime: CantonTimestamp,
      requestCounterO: Option[RequestCounter],
  )(implicit override val traceContext: TraceContext)
      extends SequencedUpdate {
    override protected def pretty: Pretty[SequencerIndexMoved] =
      prettyOfClass(
        param("domainId", _.domainId.uid),
        param("sequencerCounter", _.sequencerCounter),
        param("sequencerTimestamp", _.recordTime),
        paramIfDefined("requestCounter", _.requestCounterO),
      )
  }

  object SequencerIndexMoved {
    implicit val `SequencerIndexMoved to LoggingValue`: ToLoggingValue[SequencerIndexMoved] =
      seqIndexMoved =>
        LoggingValue.Nested.fromEntries(
          Logging.domainId(seqIndexMoved.domainId),
          "sequencerCounter" -> seqIndexMoved.sequencerCounter.unwrap,
          "sequencerTimestamp" -> seqIndexMoved.recordTime.toInstant,
        )
  }

  final case class EmptyAcsPublicationRequired(
      domainId: DomainId,
      recordTime: CantonTimestamp,
  )(implicit override val traceContext: TraceContext)
      extends DomainUpdate {
    override protected def pretty: Pretty[EmptyAcsPublicationRequired] =
      prettyOfClass(
        param("domainId", _.domainId.uid),
        param("sequencerTimestamp", _.recordTime),
      )
  }

  object EmptyAcsPublicationRequired {
    implicit val `EmptyAcsPublicationRequired to LoggingValue`
        : ToLoggingValue[EmptyAcsPublicationRequired] =
      emptyAcsPublicationRequired =>
        LoggingValue.Nested.fromEntries(
          Logging.domainId(emptyAcsPublicationRequired.domainId),
          "sequencerTimestamp" -> emptyAcsPublicationRequired.recordTime.toInstant,
        )
  }

  final case class CommitRepair()(implicit override val traceContext: TraceContext) extends Update {
    val persisted: Promise[Unit] = Promise()

    override protected def pretty: Pretty[CommitRepair] = prettyOfClass()

    override val recordTime: CantonTimestamp = CantonTimestamp.now()
  }

  implicit val `Update to LoggingValue`: ToLoggingValue[Update] = {
    case update: PartyAddedToParticipant =>
      PartyAddedToParticipant.`PartyAddedToParticipant to LoggingValue`.toLoggingValue(update)
    case update: PartyAllocationRejected =>
      PartyAllocationRejected.`PartyAllocationRejected to LoggingValue`.toLoggingValue(update)
    case update: TopologyTransactionEffective =>
      TopologyTransactionEffective.`TopologyTransactionEffective to LoggingValue`.toLoggingValue(
        update
      )
    case update: TransactionAccepted =>
      TransactionAccepted.`TransactionAccepted to LoggingValue`.toLoggingValue(update)
    case update: CommandRejected =>
      CommandRejected.`CommandRejected to LoggingValue`.toLoggingValue(update)
    case update: ReassignmentAccepted =>
      ReassignmentAccepted.`ReassignmentAccepted to LoggingValue`.toLoggingValue(update)
    case update: EmptyAcsPublicationRequired =>
      EmptyAcsPublicationRequired.`EmptyAcsPublicationRequired to LoggingValue`.toLoggingValue(
        update
      )
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

    def submitter(parties: List[Ref.Party]): LoggingEntry =
      "submitter" -> parties

    def completionInfo(info: Option[CompletionInfo]): LoggingEntry =
      "completion" -> info

    def domainId(domainId: DomainId): LoggingEntry =
      "domainId" -> domainId.toString
  }

}
