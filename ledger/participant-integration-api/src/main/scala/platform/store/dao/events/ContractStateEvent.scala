// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.platform._
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Time.Timestamp

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
  ) extends ContractStateEvent
  final case class Archived(
      contractId: ContractId,
      globalKey: Option[Key],
      stakeholders: Set[Party],
      eventOffset: Offset,
      eventSequentialId: Long,
  ) extends ContractStateEvent
}
