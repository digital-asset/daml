// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import com.google.protobuf.ByteString

object Bytes {
  def toString(bytes: ByteString): String =
    toString(bytes.toByteArray)

  def toString(bytes: Array[Byte]): String =
    bytes.map("%02x".format(_)).mkString("")
}
