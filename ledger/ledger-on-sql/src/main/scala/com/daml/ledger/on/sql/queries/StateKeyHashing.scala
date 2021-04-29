// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import com.daml.ledger.participant.state.kvutils.Raw

import java.security.MessageDigest

object StateKeyHashing {
  def hash(key: Raw.StateKey): Array[Byte] =
    hash(key.bytes.toByteArray)

  def hash(bytes: Array[Byte]): Array[Byte] =
    MessageDigest
      .getInstance("SHA-256")
      .digest(bytes)

}
