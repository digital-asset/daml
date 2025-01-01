// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.value.Value

sealed trait ReassignmentCommand {
  def sourceDomain: Source[SynchronizerId]
  def targetDomain: Target[SynchronizerId]
}

object ReassignmentCommand {
  final case class Unassign(
      sourceDomain: Source[SynchronizerId],
      targetDomain: Target[SynchronizerId],
      contractId: Value.ContractId,
  ) extends ReassignmentCommand

  final case class Assign(
      sourceDomain: Source[SynchronizerId],
      targetDomain: Target[SynchronizerId],
      unassignId: CantonTimestamp,
  ) extends ReassignmentCommand
}
