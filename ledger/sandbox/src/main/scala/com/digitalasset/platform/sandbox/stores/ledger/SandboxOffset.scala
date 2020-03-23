// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.data.Bytes

object SandboxOffset {

  def numBytes = 8

  def toOffset(value: BigInt): Offset = {
    Offset(Bytes.fromByteArray(value.toByteArray.reverse.padTo(numBytes, 0: Byte).reverse))
  }

  def fromOffset(offset: Offset): BigInt = {
    BigInt(Offset.unwrap(offset).toByteArray)
  }
}
