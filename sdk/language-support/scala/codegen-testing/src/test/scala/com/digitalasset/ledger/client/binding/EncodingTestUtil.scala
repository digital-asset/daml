// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import java.util.UUID

import com.daml.ledger.client.binding.{Primitive => P}

object EncodingTestUtil {
  def someContractId[T]: P.ContractId[T] = P.ContractId(UUID.randomUUID.toString)
}
