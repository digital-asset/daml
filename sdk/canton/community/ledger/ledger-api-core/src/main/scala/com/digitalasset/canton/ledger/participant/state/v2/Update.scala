// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.v2

import com.daml.daml_lf_dev.DamlLf
import com.daml.error.GrpcStatuses
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Bytes, Ref}
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction}
import com.daml.lf.value.Value
import com.daml.logging.entries.{LoggingEntry, LoggingValue, ToLoggingValue}
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.DomainId
import com.google.rpc.status.Status as RpcStatus

import java.time.Duration

/** An update to the (abstract) participant state.
  *
  * [[Update]]'s are used in [[ReadService.stateUpdates]] to communicate
  * changes to abstract participant state to consumers.
  *
  * We describe the possible updates in the comments of
  * each of the case classes implementing [[Update]].
  */
sealed trait Update extends Product with Serializable with PrettyPrinting {

  /** The record time at which the state change was committed. */
  def recordTime: Timestamp
}

object Update {

  /** Signal used only to increase the offset of the participant in the initialization stage. */
  final case class Init(
      recordTime: Timestamp
  ) extends Update {

    override def pretty: Pretty[Init] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        indicateOmittedFields,
      )

  }

  object Init {
    implicit val `Init to LoggingValue`: ToLoggingValue[Init] = { case Init(recordTime) =>
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
  ) extends Update {
    override def pretty: Pretty[PartyAddedToParticipant] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("party", _.party),
        param("displayName", _.displayName.singleQuoted),
        param("participantId", _.participantId),
        indicateOmittedFields,
      )
  }

  object PartyAddedToParticipant {
    implicit val `PartyAddedToParticipant to LoggingValue`
        : ToLoggingValue[PartyAddedToParticipant] = {
      case PartyAddedToParticipant(party, displayName, participantId, recordTime, submissionId) =>
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
  ) extends Update {
    override def pretty: Pretty[PartyAllocationRejected] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("participantId", _.participantId),
        param("rejectionReason", _.rejectionReason.singleQuoted),
      )
  }

  object PartyAllocationRejected {
    implicit val `PartyAllocationRejected to LoggingValue`
        : ToLoggingValue[PartyAllocationRejected] = {
      case PartyAllocationRejected(submissionId, participantId, recordTime, rejectionReason) =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(recordTime),
          Logging.submissionId(submissionId),
          Logging.participantId(participantId),
          Logging.rejectionReason(rejectionReason),
        )
    }
  }

  /** Signal that a set of new packages has been uploaded.
    *
    * @param archives          The new packages that have been accepted.
    * @param sourceDescription Description of the upload, if provided by the submitter.
    * @param recordTime        The ledger-provided timestamp at which the package upload was
    *                          committed.
    * @param submissionId      The submission id of the upload. Unset if this participant was not the
    *                          submitter.
    */
  final case class PublicPackageUpload(
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String],
      recordTime: Timestamp,
      submissionId: Option[Ref.SubmissionId],
  ) extends Update {
    override def pretty: Pretty[PublicPackageUpload] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("archives", _.archives.map(_.getHash.readableHash)),
        paramIfDefined("sourceDescription", _.sourceDescription.map(_.singleQuoted)),
      )

  }

  object PublicPackageUpload {
    implicit val `PublicPackageUpload to LoggingValue`: ToLoggingValue[PublicPackageUpload] = {
      case PublicPackageUpload(_, sourceDescription, recordTime, submissionId) =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(recordTime),
          Logging.submissionIdOpt(submissionId),
          Logging.sourceDescriptionOpt(sourceDescription),
        )
    }
  }

  /** Signal that a package upload has been rejected.
    *
    * @param submissionId    The submission id of the upload.
    * @param recordTime      The ledger-provided timestamp at which the package upload was
    *                        committed.
    * @param rejectionReason Reason why the upload was rejected.
    */
  final case class PublicPackageUploadRejected(
      submissionId: Ref.SubmissionId,
      recordTime: Timestamp,
      rejectionReason: String,
  ) extends Update {
    override def pretty: Pretty[PublicPackageUploadRejected] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("rejectionReason", _.rejectionReason.singleQuoted),
      )
  }

  object PublicPackageUploadRejected {
    implicit val `PublicPackageUploadRejected to LoggingValue`
        : ToLoggingValue[PublicPackageUploadRejected] = {
      case PublicPackageUploadRejected(submissionId, recordTime, rejectionReason) =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(recordTime),
          Logging.submissionId(submissionId),
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
    *                          The [[ReadService]] implementation must ensure that command
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
    *                          This data is opaque and can only be used in [[com.daml.lf.command.DisclosedContract]]s
    *                          when submitting transactions trough the [[WriteService]].
    *                          If a contract created by this transaction is not element of this map,
    *                          its metadata is equal to the empty byte array.
    */
  final case class TransactionAccepted(
      completionInfoO: Option[CompletionInfo],
      transactionMeta: TransactionMeta,
      transaction: CommittedTransaction,
      transactionId: Ref.TransactionId,
      recordTime: Timestamp,
      blindingInfoO: Option[BlindingInfo],
      hostedWitnesses: List[Ref.Party],
      contractMetadata: Map[Value.ContractId, Bytes],
      domainId: DomainId,
  ) extends Update {

    override def pretty: Pretty[TransactionAccepted] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("transactionId", _.transactionId),
        param("transactionMeta", _.transactionMeta),
        paramIfDefined("completion", _.completionInfoO),
        param("nodes", _.transaction.nodes.size),
        param("roots", _.transaction.roots.length),
        indicateOmittedFields,
      )
  }

  object TransactionAccepted {
    implicit val `TransactionAccepted to LoggingValue`: ToLoggingValue[TransactionAccepted] = {
      case TransactionAccepted(
            completionInfoO,
            transactionMeta,
            _,
            transactionId,
            recordTime,
            _,
            _,
            _,
            domainId,
          ) =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(recordTime),
          Logging.completionInfo(completionInfoO),
          Logging.transactionId(transactionId),
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
      updateId: Ref.TransactionId,
      recordTime: Timestamp,
      reassignmentInfo: ReassignmentInfo,
      reassignment: Reassignment,
  ) extends Update {
    override def pretty: Pretty[ReassignmentAccepted] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("updateId", _.updateId),
        paramIfDefined("completion", _.optCompletionInfo),
        param("source", _.reassignmentInfo.sourceDomain),
        param("target", _.reassignmentInfo.targetDomain),
        unnamedParam(_.reassignment.kind.unquoted),
        indicateOmittedFields,
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
          ) =>
        LoggingValue.Nested.fromEntries(
          Logging.recordTime(recordTime),
          Logging.completionInfo(optCompletionInfo),
          Logging.transactionId(updateId),
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
  ) extends Update {
    override def pretty: Pretty[CommandRejected] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("completion", _.completionInfo),
        paramIfTrue("definiteAnswer", _.definiteAnswer),
        param("reason", _.reasonTemplate.message.singleQuoted),
        param("domainId", _.domainId.uid),
      )

    /** If true, the [[ReadService]]'s deduplication guarantees apply to this rejection.
      * The participant state implementations should strive to set this flag to true as often as
      * possible so that applications get better guarantees.
      */
    def definiteAnswer: Boolean = reasonTemplate.definiteAnswer
  }

  object CommandRejected {

    implicit val `CommandRejected to LoggingValue`: ToLoggingValue[CommandRejected] = {
      case CommandRejected(recordTime, submitterInfo, reason, domainId) =>
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
        * specified for [[ReadService.stateUpdates]].
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

  implicit val `Update to LoggingValue`: ToLoggingValue[Update] = {
    case update: Init =>
      Init.`Init to LoggingValue`.toLoggingValue(update)
    case update: PartyAddedToParticipant =>
      PartyAddedToParticipant.`PartyAddedToParticipant to LoggingValue`.toLoggingValue(update)
    case update: PartyAllocationRejected =>
      PartyAllocationRejected.`PartyAllocationRejected to LoggingValue`.toLoggingValue(update)
    case update: PublicPackageUpload =>
      PublicPackageUpload.`PublicPackageUpload to LoggingValue`.toLoggingValue(update)
    case update: PublicPackageUploadRejected =>
      PublicPackageUploadRejected.`PublicPackageUploadRejected to LoggingValue`.toLoggingValue(
        update
      )
    case update: TransactionAccepted =>
      TransactionAccepted.`TransactionAccepted to LoggingValue`.toLoggingValue(update)
    case update: CommandRejected =>
      CommandRejected.`CommandRejected to LoggingValue`.toLoggingValue(update)
    case update: ReassignmentAccepted =>
      ReassignmentAccepted.`ReassignmentAccepted to LoggingValue`.toLoggingValue(update)
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

    def transactionId(id: Ref.TransactionId): LoggingEntry =
      "transactionId" -> id

    def applicationId(id: Ref.ApplicationId): LoggingEntry =
      "applicationId" -> id

    def workflowIdOpt(id: Option[Ref.WorkflowId]): LoggingEntry =
      "workflowId" -> id

    def ledgerTime(time: Timestamp): LoggingEntry =
      "ledgerTime" -> time.toInstant

    def submissionTime(time: Timestamp): LoggingEntry =
      "submissionTime" -> time.toInstant

    def configGeneration(generation: Long): LoggingEntry =
      "configGeneration" -> generation

    def maxDeduplicationDuration(time: Duration): LoggingEntry =
      "maxDeduplicationDuration" -> time

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

    def sourceDescriptionOpt(description: Option[String]): LoggingEntry =
      "sourceDescription" -> description

    def submitter(parties: List[Ref.Party]): LoggingEntry =
      "submitter" -> parties

    def completionInfo(info: Option[CompletionInfo]): LoggingEntry =
      "completion" -> info

    def domainId(domainId: DomainId): LoggingEntry =
      "domainId" -> domainId.toString
  }

}
