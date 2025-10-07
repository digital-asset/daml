// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.platform.*

sealed trait ContractStateEvent extends Product with Serializable

object ContractStateEvent {
  final case class Created(
      contract: FatContract
  ) extends ContractStateEvent {
    def contractId: ContractId = contract.contractId
    def globalKey: Option[Key] = contract.contractKeyWithMaintainers.map(_.globalKey)
  }
  final case class Archived(
      contractId: ContractId,
      globalKey: Option[Key],
      stakeholders: Set[Party],
  ) extends ContractStateEvent
  // This is merely an offset placeholder for now, sole purpose is to tick the StateCaches internal offset
  case object ReassignmentAccepted extends ContractStateEvent
}
