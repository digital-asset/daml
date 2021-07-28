// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import com.daml.ledger.configuration.Configuration
import com.daml.lf.data.Ref

/** Collects context information the submission.
  *
  * Note that this is used for party-originating changes only. They are
  * usually issued via the Ledger API.
  *
  * @param actAs               the non-empty set of parties that submitted the change.
  * @param applicationId       an identifier for the Daml application that
  *                            submitted the command. This is used for monitoring, command
  *                            deduplication, and to allow Daml applications subscribe to their own
  *                            submissions only.
  * @param commandId           a submitter-provided identifier to identify an intended ledger change
  *                            within all the submissions by the same parties and application.
  * @param deduplicationPeriod The deduplication period for the command submission.
  *                            Used for the deduplication guarantee described in the
  *                            [[ReadService.stateUpdates]].
  * @param submissionId        An identifier for the submission that allows an application to
  *                            correlate completions to its submissions.
  * @param ledgerConfiguration The ledger configuration used during interpretation
  */
final case class SubmitterInfo(
    actAs: List[Ref.Party],
    applicationId: Ref.ApplicationId,
    commandId: Ref.CommandId,
    deduplicationPeriod: DeduplicationPeriod,
    submissionId: Ref.SubmissionId,
    ledgerConfiguration: Configuration,
) {

  /** The ID for the ledger change */
  val changeId: ChangeId = ChangeId(applicationId, commandId, actAs.toSet)

  def toCompletionInfo: CompletionInfo = CompletionInfo(
    actAs,
    applicationId,
    commandId,
    Some(deduplicationPeriod),
    submissionId,
  )
}
