// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

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
  */
final case class SubmitterInfo(
    submitter: Party,
    applicationId: ApplicationId,
    commandId: CommandId,
    maxRecordTime: Timestamp //TODO: this should be a regular Instant
)
