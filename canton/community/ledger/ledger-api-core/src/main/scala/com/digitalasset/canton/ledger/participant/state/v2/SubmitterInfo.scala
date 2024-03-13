// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.v2

import com.daml.lf.data.Ref
import com.daml.lf.transaction.TransactionNodeStatistics
import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.canton.ledger.api.DeduplicationPeriod

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
  *                            [[ReadService.stateUpdates]].
  * @param submissionId        An identifier for the submission that allows an application to
  *                            correlate completions to its submissions.
  */
final case class SubmitterInfo(
    actAs: List[Ref.Party],
    readAs: List[Ref.Party],
    applicationId: Ref.ApplicationId,
    commandId: Ref.CommandId,
    deduplicationPeriod: DeduplicationPeriod,
    submissionId: Option[Ref.SubmissionId],
) {

  /** The ID for the ledger change */
  val changeId: ChangeId = ChangeId(applicationId, commandId, actAs.toSet)

  def toCompletionInfo(statistics: Option[TransactionNodeStatistics] = None): CompletionInfo =
    CompletionInfo(
      actAs,
      applicationId,
      commandId,
      Some(deduplicationPeriod),
      submissionId,
      statistics,
    )
}

object SubmitterInfo {
  implicit val `SubmitterInfo to LoggingValue`: ToLoggingValue[SubmitterInfo] = {
    case SubmitterInfo(
          actAs,
          readAs,
          applicationId,
          commandId,
          deduplicationPeriod,
          submissionId,
        ) =>
      LoggingValue.Nested.fromEntries(
        "actAs " -> actAs,
        "readAs" -> readAs,
        "applicationId " -> applicationId,
        "commandId " -> commandId,
        "deduplicationPeriod " -> deduplicationPeriod,
        "submissionId" -> submissionId,
      )
  }
}
