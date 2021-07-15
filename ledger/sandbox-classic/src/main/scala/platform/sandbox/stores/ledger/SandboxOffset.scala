// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import com.daml.ledger.offset.Offset

// Sandbox classic simply uses integral numbers starting from 1 for offsets.
// This utility object makes sure that the offset representation is always
// padded with zeros up to 8 bytes.
// This should only be used in SqlLedger and InMemoryLedger.
private[ledger] object SandboxOffset {

  def numBytes = 8

  def toOffset(value: BigInt): Offset = {
    Offset.fromByteArray(value.toByteArray.reverse.padTo(numBytes, 0: Byte).reverse)
  }

  def fromOffset(offset: Offset): BigInt = {
    BigInt(offset.toByteArray)
  }
}
