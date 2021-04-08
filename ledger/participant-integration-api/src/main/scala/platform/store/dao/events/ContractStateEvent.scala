// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.transaction.GlobalKey

sealed trait ContractStateEvent extends Product with Serializable {
  def eventOffset: Offset
  def eventSequentialId: Long
}
object ContractStateEvent {
  final case class Created(
      contractId: ContractId,
      contract: Contract,
      globalKey: Option[GlobalKey],
      stakeholders: Set[Party],
      eventOffset: Offset,
      eventSequentialId: Long,
  ) extends ContractStateEvent
  final case class Archived(
      contractId: ContractId,
      globalKey: Option[GlobalKey],
      stakeholders: Set[Party],
      eventOffset: Offset,
      eventSequentialId: Long,
  ) extends ContractStateEvent
  final case class LedgerEndMarker(
      eventOffset: Offset,
      eventSequentialId: Long,
  ) extends ContractStateEvent
}
