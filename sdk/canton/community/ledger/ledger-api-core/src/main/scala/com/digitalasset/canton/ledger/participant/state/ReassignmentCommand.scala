// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.value.Value

sealed trait ReassignmentCommand {
  def sourceSynchronizer: Source[SynchronizerId]
  def targetSynchronizer: Target[SynchronizerId]
}

object ReassignmentCommand {
  final case class Unassign(
      sourceSynchronizer: Source[SynchronizerId],
      targetSynchronizer: Target[SynchronizerId],
      contractId: Value.ContractId,
  ) extends ReassignmentCommand

  final case class Assign(
      sourceSynchronizer: Source[SynchronizerId],
      targetSynchronizer: Target[SynchronizerId],
      unassignId: CantonTimestamp,
  ) extends ReassignmentCommand
}
