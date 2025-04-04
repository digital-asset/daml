// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.daml.lf.data.Ref

/** Information about a completion for a submission.
  *
  * @param actAs
  *   the non-empty set of parties that submitted the change.
  * @param userId
  *   an identifier for the user that submitted the command.
  * @param commandId
  *   a submitter-provided identifier to identify an intended ledger change within all the
  *   submissions by the same parties and application.
  * @param optDeduplicationPeriod
  *   The deduplication period that the [[SyncService]] actually uses for the command submission. It
  *   may differ from the suggested deduplication period given to [[SyncService.submitTransaction]].
  *
  * For example, the suggested deduplication period may have been converted into a different kind or
  * extended. The particular choice depends on the particular implementation.
  *
  * This allows auditing the deduplication guarantee described in the [[Update]].
  *
  * Optional as some implementations may not be able to provide this deduplication information. If
  * an implementation does not provide this deduplication information, it MUST adhere to the
  * deduplication guarantee under a sensible interpretation of the corresponding
  * [[CompletionInfo.optDeduplicationPeriod]].
  * @param submissionId
  *   An identifier for the submission that allows an application to correlate completions to its
  *   submissions.
  *
  * Optional as entries created by the participant.state.v1 API do not have this filled. Only set
  * for participant.state.v2 created entries
  */
final case class CompletionInfo(
    actAs: List[Ref.Party],
    userId: Ref.UserId,
    commandId: Ref.CommandId,
    optDeduplicationPeriod: Option[DeduplicationPeriod],
    submissionId: Option[Ref.SubmissionId],
) extends PrettyPrinting {
  def changeId: ChangeId = ChangeId(userId, commandId, actAs.toSet)

  override protected def pretty: Pretty[CompletionInfo.this.type] = prettyOfClass(
    param("actAs", _.actAs.mkShow()),
    param("commandId", _.commandId),
    param("userId", _.userId),
    paramIfDefined("deduplication period", _.optDeduplicationPeriod),
    param("submissionId", _.submissionId),
    indicateOmittedFields,
  )
}

object CompletionInfo {
  implicit val `CompletionInfo to LoggingValue`: ToLoggingValue[CompletionInfo] = {
    case CompletionInfo(actAs, userId, commandId, deduplicationPeriod, submissionId) =>
      LoggingValue.Nested.fromEntries(
        "actAs " -> actAs,
        "userId " -> userId,
        "commandId " -> commandId,
        "deduplicationPeriod " -> deduplicationPeriod,
        "submissionId" -> submissionId,
      )
  }
}
