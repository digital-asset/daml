// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.platform.*

sealed trait ContractStateEvent extends Product with Serializable {
  def eventOffset: Offset
  def eventSequentialId: Long
}

object ContractStateEvent {
  final case class Created(
      contractId: ContractId,
      contract: Contract,
      globalKey: Option[Key],
      ledgerEffectiveTime: Timestamp,
      stakeholders: Set[Party],
      eventOffset: Offset,
      eventSequentialId: Long,
      signatories: Set[Party],
      keyMaintainers: Option[Set[Party]],
      driverMetadata: Option[Array[Byte]],
  ) extends ContractStateEvent
  final case class Archived(
      contractId: ContractId,
      globalKey: Option[Key],
      stakeholders: Set[Party],
      eventOffset: Offset,
      eventSequentialId: Long,
  ) extends ContractStateEvent
}
