// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import com.daml.lf.data.Time.Timestamp
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.transaction.BlindingInfo

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/** An update to the (abstract) participant state.
  *
  * [[Update]]'s are used in [[ReadService.stateUpdates]] to communicate
  * changes to abstract participant state to consumers.
  *
  * We describe the possible updates in the comments of
  * each of the case classes implementing [[Update]].
  */
sealed trait Update extends Product with Serializable {

  /** Short human-readable one-line description summarizing the state updates content. */
  def description: String

  /** The record time at which the state change was committed. */
  def recordTime: Timestamp
}

object Update {

  /** Signal that the current [[Configuration]] has changed. */
  final case class ConfigurationChanged(
      recordTime: Timestamp,
      submissionId: AdminSubmissionId,
      participantId: ParticipantId,
      newConfiguration: Configuration,
  ) extends Update {
    override def description: String =
      s"Configuration change '$submissionId' from participant '$participantId' accepted with configuration: $newConfiguration"
  }

  /** Signal that a configuration change submitted by this participant was rejected.
    */
  final case class ConfigurationChangeRejected(
      recordTime: Timestamp,
      submissionId: AdminSubmissionId,
      participantId: ParticipantId,
      proposedConfiguration: Configuration,
      rejectionReason: String,
  ) extends Update {
    override def description: String = {
      s"Configuration change '$submissionId' from participant '$participantId' was rejected: $rejectionReason"
    }
  }

  /** Signal that a party is hosted at a participant.
    *
    * @param party
    *   The newly allocated party identifier.
    *
    * @param displayName
    *   The user readable description of the party. May not be unique.
    *
    * @param participantId
    *   The participant that this party was added to.
    *
    * @param recordTime
    *   The ledger-provided timestamp at which the party was allocated.
    *
    * @param submissionId
    *   The submissionId of the command which requested party to be added.
    */
  final case class PartyAddedToParticipant(
      party: Party,
      displayName: String,
      participantId: ParticipantId,
      recordTime: Timestamp,
      submissionId: Option[AdminSubmissionId],
  ) extends Update {
    override def description: String =
      s"Add party '$party' to participant"
  }

  /** Signal that the party allocation request has been Rejected.
    *
    * Initially this will be visible to all participants in the current open world,
    * with a possible need to revisit as part of the per-party package visibility work
    * https://github.com/digital-asset/daml/issues/311.
    *
    * @param submissionId
    *   submissionId of the party allocation command.
    *
    * @param participantId
    *   The participant to which the party was requested to be added. This field
    *   is informative,
    *
    * @param recordTime
    *   The ledger-provided timestamp at which the party was added.
    *
    * @param rejectionReason
    *   reason for rejection of the party allocation entry
    *
    * Consider whether an enumerated set of reject reasons a la [[RejectionReason]] would be helpful, and whether the same breadth of reject
    * types needs to be handled for party allocation entry rejects
    */
  final case class PartyAllocationRejected(
      submissionId: AdminSubmissionId,
      participantId: ParticipantId,
      recordTime: Timestamp,
      rejectionReason: String,
  ) extends Update {
    override val description: String =
      s"Request to add party to participant with submissionId '$submissionId' failed"
  }

  /** Signal that a set of new packages has been uploaded.
    *
    * @param archives:
    *   The new packages that have been accepted.
    * @param sourceDescription:
    *   Description of the upload, if provided by the submitter.
    * @param recordTime:
    *   The ledger-provided timestamp at which the package upload was committed.
    * @param submissionId:
    *   The submission id of the upload. Unset if this participant was not the submitter.
    */
  final case class PublicPackageUpload(
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String],
      recordTime: Timestamp,
      submissionId: Option[AdminSubmissionId],
  ) extends Update {
    override def description: String =
      s"Public package upload: ${archives.map(_.getHash).mkString(", ")}"
  }

  /** Signal that a package upload has been rejected.
    *
    * @param submissionId:
    *   The submission id of the upload.
    * @param recordTime:
    *   The ledger-provided timestamp at which the package upload was committed.
    * @param rejectionReason:
    *   Reason why the upload was rejected.
    */
  final case class PublicPackageUploadRejected(
      submissionId: AdminSubmissionId,
      recordTime: Timestamp,
      rejectionReason: String,
  ) extends Update {
    override def description: String =
      s"Public package upload rejected, correlationId=$submissionId reason='$rejectionReason'"
  }

  /** Signal the acceptance of a transaction.
    *
    * @param optCompletionInfo:
    *   The information provided by the submitter of the command that
    *   created this transaction. It must be provided if this participant
    *   hosts one of the [[SubmitterInfo.actAs]] parties and shall output a completion event
    *   for this transaction. This in particular applies if this participant has
    *   submitted the command to the [[WriteService]].
    *
    *   The [[ReadService]] implementation must ensure that command deduplication
    *   and the submission rank guarantees are met.
    *
    * @param transactionMeta:
    *   The metadata of the transaction that was provided by the submitter.
    *   It is visible to all parties that can see the transaction.
    *
    * @param transaction:
    *   The view of the transaction that was accepted. This view must
    *   include at least the projection of the accepted transaction to the
    *   set of all parties hosted at this participant. See
    *   https://docs.daml.com/concepts/ledger-model/ledger-privacy.html
    *   on how these views are computed.
    *
    *   Note that ledgers with weaker privacy models can decide to forgo
    *   projections of transactions and always show the complete
    *   transaction.
    *
    * @param recordTime:
    *   The ledger-provided timestamp at which the transaction was recorded.
    *   The last [[Configuration]] set before this [[TransactionAccepted]]
    *   determines how this transaction's recordTime relates to its
    *   [[TransactionMeta.ledgerEffectiveTime]].
    *
    * @param divulgedContracts:
    *   List of divulged contracts. See [[DivulgedContract]] for details.
    */
  final case class TransactionAccepted(
      optCompletionInfo: Option[CompletionInfo],
      transactionMeta: TransactionMeta,
      transaction: CommittedTransaction,
      transactionId: TransactionId,
      recordTime: Timestamp,
      divulgedContracts: List[DivulgedContract],
      blindingInfo: Option[BlindingInfo],
  ) extends Update {
    override def description: String = s"Accept transaction $transactionId"
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
  ) extends Update {
    override def description: String =
      s"Reject command ${completionInfo.commandId}${if (definiteAnswer)
        " (definite answer)"}: ${reasonTemplate.message}"

    /** If true, the [[ReadService]]'s deduplication and submission rank guarantees
      * apply to this rejection. The participant state implementations should
      * strive to set this flag to true as often as possible so that applications
      * get better guarantees.
      */
    def definiteAnswer: Boolean = reasonTemplate.definiteAnswer
  }

  object CommandRejected {

    /** A template for generating gRPC status codes.
      * The indexer server should provide some details
      * before the [[FinalReason]] gives an actual gRPC status code.
      */
    sealed trait RejectionReasonTemplate {

      /** Whether the rejection is a definite answer for the deduplication and rank guarantees
        * specified for [[ReadService.stateUpdates]].
        */
      def definiteAnswer: Boolean

      /** A human-readable description of the error */
      def message: String
    }

    object RejectionReasonTemplate {
      val definiteAnswerKey = "definite_answer"

      def isDefiniteAnswer(status: com.google.rpc.status.Status): Boolean =
        status.details.exists { any =>
          if (any.is(com.google.rpc.error_details.ErrorInfo.messageCompanion)) {
            Try(any.unpack(com.google.rpc.error_details.ErrorInfo.messageCompanion))
              .toOption
              .exists(isDefiniteAnswer)
          } else false
        }

      def isDefiniteAnswer(
          errorInfo: com.google.rpc.error_details.ErrorInfo): Boolean =
        errorInfo.metadata.getOrElse(definiteAnswerKey, "false") == "true"
    }

    /** The status code for the command rejection. */
    final class FinalReason(val status: com.google.rpc.status.Status)
        extends RejectionReasonTemplate {

      override def message: String = status.message
      override def definiteAnswer: Boolean =
        RejectionReasonTemplate.isDefiniteAnswer(status)
    }

    /** The indexer shall fill in a completion offset for the completion that corresponds to
      * the `submissionId` and `submissionRank` by calling `status` with the completion offset. If no completion offset
      * for the `submissionId` can be provided, [[scala.None$]] can be used instead,
      * which may lead to less informative errors.
      */
    final class NeedCompletionOffsetForSubmissionId(
        val submissionId: SubmissionId,
        val submissionRank: Offset,
        private val completionKey: String,
        private val incompleteStatus: com.google.rpc.status.Status,
    ) extends RejectionReasonTemplate {

      require(completionKey != RejectionReasonTemplate.definiteAnswerKey)

      private val (errorInfo, errorInfoIndex): (com.google.rpc.error_details.ErrorInfo, Int) = {
        val iterator = incompleteStatus.details.iterator

        @tailrec def go(index: Int): Option[(com.google.rpc.error_details.ErrorInfo, Int)] =
          if (iterator.hasNext) {
            val next = iterator.next()
            if (next.is(com.google.rpc.error_details.ErrorInfo.messageCompanion)) {
              Try(next.unpack(com.google.rpc.error_details.ErrorInfo.messageCompanion)) match {
                case Success(errorInfo) => Some(errorInfo -> index)
                case _: Failure[_]      => go(index + 1)
              }
            } else go(index + 1)
          } else None

        go(0).getOrElse(
          throw new IllegalArgumentException(
            s"No com.google.rpc.error_details.ErrorInfo found in details for $incompleteStatus"
          )
        )
      }

      override def message: String = incompleteStatus.message

      override def definiteAnswer: Boolean =
        RejectionReasonTemplate.isDefiniteAnswer(errorInfo)

      def status(
          completionOffsetForSubmissionIdAndRank: Option[Offset]
      ): com.google.rpc.status.Status =
        completionOffsetForSubmissionIdAndRank.fold(incompleteStatus) {
          completionOffset =>
            val newErrorInfo =
              errorInfo
                .copy()
                .addMetadata(completionKey -> completionOffset.toHexString)
            val newDetails = incompleteStatus.details.updated(
              errorInfoIndex,
              com.google.protobuf.any.Any.pack(newErrorInfo),
            )
            incompleteStatus.withDetails(newDetails)
        }
    }
  }
}
