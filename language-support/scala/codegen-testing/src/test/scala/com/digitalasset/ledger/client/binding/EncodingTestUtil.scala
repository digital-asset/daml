// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import java.util.UUID

import com.digitalasset.ledger.client.binding.{Primitive => P}

object EncodingTestUtil {
  def someContractId[T]: P.ContractId[T] = P.ContractId(UUID.randomUUID.toString)
}
