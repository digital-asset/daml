// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker

/** Some of the state of a participant that is not tied to a synchronizer and is kept only in
  * memory.
  */
class ParticipantNodeEphemeralState(
    val inFlightSubmissionTracker: InFlightSubmissionTracker
)

object ParticipantNodeEphemeralState {
  def apply(
      inFlightSubmissionTracker: InFlightSubmissionTracker
  ): ParticipantNodeEphemeralState =
    new ParticipantNodeEphemeralState(inFlightSubmissionTracker)
}
