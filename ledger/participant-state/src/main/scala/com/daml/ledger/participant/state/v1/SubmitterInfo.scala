// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.time.Instant

import com.digitalasset.daml.lf.data.Time.Timestamp

/** Information provided by the submitter of changes submitted to the ledger.
  *
  * Note that this is used for party-originating changes only. They are
  * usually issued via the Ledger API.
  *
  * @param submitter: the party that submitted the change.
  *
  * @param applicationId: an identifier for the DAML application that
  *   submitted the command. This is used for monitoring and to allow DAML
  *   applications subscribe to their own submissions only.
  *
  * @param commandId: a submitter provided identifier that he can use to
  *   correlate the stream of changes to the participant state with the
  *   changes he submitted.
  *
  * @param maxRecordTime: the maximum record time (inclusive) until which
  *   the submitted change can be validly added to the ledger. This is used
  *   by DAML applications to deduce from the record time reported by the
  *   ledger whether a change that they submitted has been lost in transit.
  *
  * @param deduplicateUntil: the time until which the command should be deduplicated.
  *   Command deduplication is already performed by the participant.
  *   The ledger may choose to perform additional (cross-participant)
  *   command deduplication. If it chooses to do so, it must follow the
  *   same rules as the participant:
  *   - Deduplication is based on the (submitter, commandId) tuple.
  *   - Commands must not be deduplicated after the `deduplicateUntil` time has passed.
  *   - Commands should not be deduplicated after the command was rejected.
  */
final case class SubmitterInfo(
    submitter: Party,
    applicationId: ApplicationId,
    commandId: CommandId,
    maxRecordTime: Timestamp, //TODO: this should be a regular Instant
    deduplicateUntil: Instant,
)
