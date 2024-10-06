// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.protocol.{
  ExternallySignedTransaction,
  TransactionAuthorizationPartySignatures,
}
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
  */
final case class SubmitterInfo(
    actAs: List[Ref.Party],
    readAs: List[Ref.Party],
    applicationId: Ref.ApplicationId,
    commandId: Ref.CommandId,
    deduplicationPeriod: DeduplicationPeriod,
    submissionId: Option[Ref.SubmissionId],
    externallySignedTransaction: Option[ExternallySignedTransaction],
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
  implicit val `PartySignatures to LoggingValue`
      : ToLoggingValue[TransactionAuthorizationPartySignatures] = {
    case TransactionAuthorizationPartySignatures(signatures) =>
      LoggingValue.Nested.fromEntries(
        "parties" -> signatures.map(_._1.toProtoPrimitive)
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
          externallySignedTransaction,
        ) =>
      LoggingValue.Nested.fromEntries(
        "actAs " -> actAs,
        "readAs" -> readAs,
        "applicationId " -> applicationId,
        "commandId " -> commandId,
        "deduplicationPeriod " -> deduplicationPeriod,
        "submissionId" -> submissionId,
        "partySignatures" -> externallySignedTransaction.map(_.signatures),
      )
  }
}
