// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import com.daml.lf.data.Ref

/** Information about a completion for a submission.
  * Unless noted otherwise, the data is the same as for [[SubmitterInfo]].
  *
  * @param actAs                  the non-empty set of parties that submitted the change.
  * @param applicationId          an identifier for the Daml application that submitted the command.
  * @param commandId              a submitter-provided identifier to identify an intended ledger change
  *                               within all the submissions by the same parties and application.
  * @param optDeduplicationPeriod The deduplication period that the [[WriteService]] actually uses for the command submission.
  *                               It may differ from the suggested deduplication period given to [[WriteService.submitTransaction]] or [[WriteService.rejectSubmission]].
  *                               For example, the suggested deduplication period may have been converted into a different kind or extended.
  *                               The particular choice depends on the particular implementation.
  *                               This allows auditing the deduplication guarantee described in the [[ReadService.stateUpdates]].
  *
  *                               Optional as some implementations may not be able to provide this deduplication information.
  *                               If an implementation does not provide this deduplication information,
  *                               it MUST adhere to the deduplication guarantee under a sensible interpretation
  *                               of the corresponding [[SubmitterInfo.deduplicationPeriod]].
  * @param submissionId An identifier for the submission that allows an application
  *   to correlate completions to its submissions.
  */
case class CompletionInfo(
    actAs: List[Ref.Party],
    applicationId: Ref.ApplicationId,
    commandId: Ref.CommandId,
    optDeduplicationPeriod: Option[DeduplicationPeriod],
    submissionId: Ref.SubmissionId,
) {
  def changeId: ChangeId = ChangeId(applicationId, commandId, actAs.toSet)
}
