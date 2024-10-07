// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.value.Value

sealed trait ReassignmentCommand {
  def sourceDomain: Source[DomainId]
  def targetDomain: Target[DomainId]
}

object ReassignmentCommand {
  final case class Unassign(
      sourceDomain: Source[DomainId],
      targetDomain: Target[DomainId],
      contractId: Value.ContractId,
  ) extends ReassignmentCommand

  final case class Assign(
      sourceDomain: Source[DomainId],
      targetDomain: Target[DomainId],
      unassignId: CantonTimestamp,
  ) extends ReassignmentCommand
}
