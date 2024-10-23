// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.ledger.api
package internal.host

import org.teavm.interop.{Address, Structure}

private[api] class ByteString private (val ptr: Address, val size: Int) extends Structure {
  def toByteArray: Array[Byte] = {
    (0 to size).map(offset => ptr.add(offset).getByte()).toArray[Byte]
  }
}

private[api] object ByteString {
  def apply(bytes: Array[Byte]): ByteString = {
    new ByteString(Address.ofData(bytes), bytes.length)
  }

  def apply(str: String): ByteString = {
    ByteString(str.getBytes)
  }
}
