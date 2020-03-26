// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.daml.ledger.participant.state.v1.Offset

object SandboxOffset {

  def numBytes = 8

  def toOffset(value: BigInt): Offset = {
    Offset.fromByteArray(value.toByteArray.reverse.padTo(numBytes, 0: Byte).reverse)
  }

  def fromOffset(offset: Offset): BigInt = {
    BigInt(offset.toByteArray)
  }
}
