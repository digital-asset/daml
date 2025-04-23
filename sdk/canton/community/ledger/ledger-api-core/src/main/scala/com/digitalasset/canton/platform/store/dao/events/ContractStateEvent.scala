// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.*
import com.digitalasset.daml.lf.data.Time.Timestamp

sealed trait ContractStateEvent extends Product with Serializable {
  def eventOffset: Offset
}

object ContractStateEvent {
  final case class Created(
      contractId: ContractId,
      contract: ThinContract,
      globalKey: Option[Key],
      ledgerEffectiveTime: Timestamp,
      stakeholders: Set[Party],
      eventOffset: Offset,
      signatories: Set[Party],
      keyMaintainers: Option[Set[Party]],
      driverMetadata: Array[Byte],
  ) extends ContractStateEvent
  final case class Archived(
      contractId: ContractId,
      globalKey: Option[Key],
      stakeholders: Set[Party],
      eventOffset: Offset,
  ) extends ContractStateEvent
  // This is merely an offset placeholder for now, sole purpose is to tick the StateCaches internal offset
  final case class ReassignmentAccepted(
      eventOffset: Offset
  ) extends ContractStateEvent
}
