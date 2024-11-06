// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.canton.crypto.{Hash, Signature}
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo.ExternallySignedSubmission
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.daml.lf.data.Ref

/** Collects context information for a submission.
  *
  * Note that this is used for party-originating changes only. They are usually issued via the
  * Ledger API.
  *
  * @param actAs                the non-empty set of parties that submitted the change.
  * @param readAs               the parties on whose behalf (in addition to all parties listed in [[actAs]]) contracts can be retrieved.
  * @param applicationId        an identifier for the Daml application that
  *                             submitted the command. This is used for monitoring, command
  *                             deduplication, and to allow Daml applications subscribe to their own
  *                             submissions only.
  * @param commandId            a submitter-provided identifier to identify an intended ledger change
  *                             within all the submissions by the same parties and application.
  * @param deduplicationPeriod  The deduplication period for the command submission.
  *                             Used for the deduplication guarantee described in the
  *                             [[Update]].
  * @param submissionId         An identifier for the submission that allows an application to
  *                             correlate completions to its submissions.
  * @param externallySignedSubmission If this is provided then the authorization for all acting parties
  *                                   will be provided by the enclosed signatures.
  */
final case class SubmitterInfo(
    actAs: List[Ref.Party],
    readAs: List[Ref.Party],
    applicationId: Ref.ApplicationId,
    commandId: Ref.CommandId,
    deduplicationPeriod: DeduplicationPeriod,
    submissionId: Option[Ref.SubmissionId],
    externallySignedSubmission: Option[ExternallySignedSubmission],
) {

  /** The ID for the ledger change */
  val changeId: ChangeId = ChangeId(applicationId, commandId, actAs.toSet)

  def toCompletionInfo: CompletionInfo =
    CompletionInfo(
      actAs,
      applicationId,
      commandId,
      Some(deduplicationPeriod),
      submissionId,
      None,
    )

}

object SubmitterInfo {
  implicit val `ExternallySignedSubmission to LoggingValue`
      : ToLoggingValue[ExternallySignedSubmission] = {
    case ExternallySignedSubmission(_, signatures) =>
      LoggingValue.Nested.fromEntries(
        "signatures" -> signatures.keys.map(_.toProtoPrimitive)
      )
  }
  implicit val `SubmitterInfo to LoggingValue`: ToLoggingValue[SubmitterInfo] = {
    case SubmitterInfo(
          actAs,
          readAs,
          applicationId,
          commandId,
          deduplicationPeriod,
          submissionId,
          externallySignedSubmission,
        ) =>
      LoggingValue.Nested.fromEntries(
        "actAs " -> actAs,
        "readAs" -> readAs,
        "applicationId " -> applicationId,
        "commandId " -> commandId,
        "deduplicationPeriod " -> deduplicationPeriod,
        "submissionId" -> submissionId,
        "externallySignedSubmission" -> externallySignedSubmission,
      )
  }

  final case class ExternallySignedSubmission(
      hash: Hash,
      signatures: Map[PartyId, Seq[Signature]],
  )

}
