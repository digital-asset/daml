// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}

import scala.concurrent.Future

trait ContractPayloadStore {

  def loadContractPayloads(
      contractId: Set[ContractId]
  ): Future[Map[ContractId, VersionedContractInstance]]

}
