// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.data.{CantonTimestamp, Counter}
import com.digitalasset.canton.protocol.{ReassignmentId, SerializableContract}
import com.digitalasset.canton.util.ReassignmentTag.Source
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{RequestCounter, RequestCounterDiscriminator}

/** Stores the data for assignment in the special case where no reassignment data is present. */
final case class AssignmentData(
    reassignmentId: ReassignmentId,
    contract: SerializableContract,
    sourceProtocolVersion: Source[ProtocolVersion],
) {
  def unassignmentRequestCounter: Counter[RequestCounterDiscriminator] = RequestCounter.MaxValue
  def unassignmentDecisionTime: CantonTimestamp = CantonTimestamp.Epoch
}
