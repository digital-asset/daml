// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.topology.ParticipantId

trait HasSubmissionTrackerData extends PrettyPrinting {
  def submissionTrackerData: Option[SubmissionTrackerData]
}

/** submissionTrackerData extends HasSubmissionTrackerData because it's used as the ViewSubmitterMetadata in the ViewTypeTest. */
final case class SubmissionTrackerData(
    submittingParticipant: ParticipantId,
    maxSequencingTime: CantonTimestamp,
) extends HasSubmissionTrackerData {
  override def submissionTrackerData: Option[SubmissionTrackerData] = Some(this)
  override protected def pretty: Pretty[SubmissionTrackerData] = prettyOfClass(
    param("submitting participant", _.submittingParticipant),
    param("max sequencing time", _.maxSequencingTime),
  )
}
