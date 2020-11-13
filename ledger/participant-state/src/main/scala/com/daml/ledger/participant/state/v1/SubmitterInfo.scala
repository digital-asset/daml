// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.time.Instant

/** Information provided by the submitter of changes submitted to the ledger.
  *
  * Note that this is used for party-originating changes only. They are
  * usually issued via the Ledger API.
  *
  * @param actAs: the non-empty set of parties that submitted the change.
  *
  *
  * @param applicationId: an identifier for the DAML application that
  *   submitted the command. This is used for monitoring and to allow DAML
  *   applications subscribe to their own submissions only.
  *
  * @param commandId: a submitter provided identifier that he can use to
  *   correlate the stream of changes to the participant state with the
  *   changes he submitted.
  *
  * @param deduplicateUntil: the time until which the command should be deduplicated.
  *   Command deduplication is already performed by the participant.
  *   The ledger may choose to perform additional (cross-participant)
  *   command deduplication. If it chooses to do so, it must follow the
  *   same rules as the participant:
  *   - Deduplication is based on the (submitter, commandId) tuple.
  *   - Commands must not be deduplicated after the deduplicateUntil time has passed.
  *   - Commands should not be deduplicated after the command was rejected.
  */
final case class SubmitterInfo(
    actAs: List[Party],
    applicationId: ApplicationId,
    commandId: CommandId,
    deduplicateUntil: Instant,
) {
  // Note: this function is only available temporarily until the entire DAML code base
  // supports multi-party submissions. Use at your own risk.
  def singleSubmitterOrThrow(): Party = {
    if (actAs.length == 1)
      actAs.head
    else
      throw new RuntimeException("SubmitterInfo contains more than one acting party")
  }
}

object SubmitterInfo {
  // Note: this function is only available temporarily until the entire DAML code base
  // supports multi-party submissions. Use at your own risk.
  def withSingleSubmitter(
      submitter: Party,
      applicationId: ApplicationId,
      commandId: CommandId,
      deduplicateUntil: Instant,
  ): SubmitterInfo = SubmitterInfo.apply(
    actAs = List(submitter),
    applicationId = applicationId,
    commandId = commandId,
    deduplicateUntil = deduplicateUntil,
  )
}
