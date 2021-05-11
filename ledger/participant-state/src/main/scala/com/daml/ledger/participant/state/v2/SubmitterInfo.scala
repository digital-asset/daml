// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.time.Instant

import com.daml.ledger.participant.state.v1.Offset

/** Information provided by the submitter of changes submitted to the ledger.
  *
  * Note that this is used for party-originating changes only. They are
  * usually issued via the Ledger API.
  *
  * @param actAs the non-empty set of parties that submitted the change.
  *
  * @param applicationId an identifier for the DAML application that
  *   submitted the command. This is used for monitoring and to allow DAML
  *   applications subscribe to their own submissions only.
  *
  * @param commandId a submitter provided identifier that he can use to
  *   correlate the stream of changes to the participant state with the
  *   changes he submitted.
  *
  * @param deduplicationPeriod The deduplication period for the command submission.
  *   If not given explicitly as an offset, the [[WriteService]] is responsible for
  *   determining a suitable completion offset where deduplication starts according
  *   to the [[ReadService]]'s deduplication guarantee.
  *
  * @param submissionId An identifier for the submission that allows an application to correlate completions to its submissions.
  *
  * @param submissionRank The rank of the submission among all submissions with the same change ID.
  *                       Used for the submission rank guarantee described in the [[ReadService.stateUpdates]].
  */
final case class SubmitterInfo(
    actAs: List[Party],
    applicationId: ApplicationId,
    commandId: CommandId,
    deduplicationPeriod: DeduplicationPeriod,
    submissionId: SubmissionId,
    submissionRank: Offset,
) {
  def changeId: ChangeId = ChangeId(applicationId, commandId, actAs = actAs.toSet)
}

case class ChangeId(applicationId: ApplicationId, commandId: CommandId, actAs: Set[Party])
