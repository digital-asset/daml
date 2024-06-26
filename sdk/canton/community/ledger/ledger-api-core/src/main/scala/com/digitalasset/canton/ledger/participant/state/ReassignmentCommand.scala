// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{SourceDomainId, TargetDomainId}
import com.digitalasset.daml.lf.value.Value

sealed trait ReassignmentCommand {
  def sourceDomain: SourceDomainId
  def targetDomain: TargetDomainId
}

object ReassignmentCommand {
  final case class Unassign(
      sourceDomain: SourceDomainId,
      targetDomain: TargetDomainId,
      contractId: Value.ContractId,
  ) extends ReassignmentCommand

  final case class Assign(
      sourceDomain: SourceDomainId,
      targetDomain: TargetDomainId,
      unassignId: CantonTimestamp,
  ) extends ReassignmentCommand
}
