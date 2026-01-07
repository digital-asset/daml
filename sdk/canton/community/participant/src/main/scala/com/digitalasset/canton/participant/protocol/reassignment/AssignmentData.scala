// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.data.{CantonTimestamp, ContractsReassignmentBatch}
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.Source

/** Stores the data for assignment in the special case where no reassignment data is present. */
final case class AssignmentData(
    reassignmentId: ReassignmentId,
    sourceSynchronizer: Source[SynchronizerId],
    contracts: ContractsReassignmentBatch,
) {
  def unassignmentDecisionTime: CantonTimestamp = CantonTimestamp.Epoch
}
