// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo.ExternallySignedSubmission
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.data.Ref

import java.util.UUID

/** Collects context information for a submission.
  *
  * Note that this is used for party-originating changes only. They are usually issued via the
  * Ledger API.
  *
  * @param actAs
  *   the non-empty set of parties that submitted the change.
  * @param readAs
  *   the parties on whose behalf (in addition to all parties listed in [[actAs]]) contracts can be
  *   retrieved.
  * @param userId
  *   an identifier for the user that submitted the command. This is used for monitoring, command
  *   deduplication, and to allow Daml applications subscribe to their own submissions only.
  * @param commandId
  *   a submitter-provided identifier to identify an intended ledger change within all the
  *   submissions by the same parties and application.
  * @param deduplicationPeriod
  *   The deduplication period for the command submission. Used for the deduplication guarantee
  *   described in the [[Update]].
  * @param submissionId
  *   An identifier for the submission that allows an application to correlate completions to its
  *   submissions.
  * @param transactionUUID
  *   Optionally explicitly chosen TransactionUUID. This is set in externally signed transactions
  *   where the external party has included the transactionUUID in the signature. It acts as a
  *   replay protection mechanism by allowing the mediator to deduplicate requests.
  * @param mediatorGroup
  *   Optionally explicitly chosen mediator group. This is set in externally signed transactions
  *   where the external party has included the mediator group in the signature.
  *
  * @param externallySignedSubmission
  *   If this is provided then the authorization for all acting parties will be provided by the
  *   enclosed signatures.
  */
final case class SubmitterInfo(
    actAs: List[Ref.Party],
    readAs: List[Ref.Party],
    userId: Ref.UserId,
    commandId: Ref.CommandId,
    deduplicationPeriod: DeduplicationPeriod,
    submissionId: Option[Ref.SubmissionId],
    externallySignedSubmission: Option[ExternallySignedSubmission],
) {

  /** The ID for the ledger change */
  val changeId: ChangeId = ChangeId(userId, commandId, actAs.toSet)

  def toCompletionInfo: CompletionInfo =
    CompletionInfo(
      actAs,
      userId,
      commandId,
      Some(deduplicationPeriod),
      submissionId,
    )

}

object SubmitterInfo {
  implicit val `ExternallySignedSubmission to LoggingValue`
      : ToLoggingValue[ExternallySignedSubmission] = {
    case ExternallySignedSubmission(
          version,
          signatures,
          transactionUUID,
          mediatorGroup,
          usesLedgerEffectiveTime,
        ) =>
      LoggingValue.Nested.fromEntries(
        "version" -> version.index,
        "signatures" -> signatures.keys.map(_.toProtoPrimitive),
        "transactionUUID" -> transactionUUID.toString,
        "mediatorGroup" -> mediatorGroup.toString,
        "usesLedgerEffectiveTime" -> usesLedgerEffectiveTime,
      )
  }
  implicit val `SubmitterInfo to LoggingValue`: ToLoggingValue[SubmitterInfo] = {
    case SubmitterInfo(
          actAs,
          readAs,
          userId,
          commandId,
          deduplicationPeriod,
          submissionId,
          externallySignedSubmission,
        ) =>
      LoggingValue.Nested.fromEntries(
        "actAs " -> actAs,
        "readAs" -> readAs,
        "userId " -> userId,
        "commandId " -> commandId,
        "deduplicationPeriod " -> deduplicationPeriod,
        "submissionId" -> submissionId,
        "externallySignedSubmission" -> externallySignedSubmission,
      )
  }

  final case class ExternallySignedSubmission(
      version: HashingSchemeVersion,
      signatures: Map[PartyId, Seq[Signature]],
      transactionUUID: UUID,
      mediatorGroup: MediatorGroupIndex,
      usesLedgerEffectiveTime: Boolean,
  )

}
