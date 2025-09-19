// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto.rlp

/**
 * @author Roman Mandeleil
 * @since 21.04.14
 */
class RLPItem(private val rlpData: Array[Byte]) extends Nothing {
  def getRLPData: Array[Byte] = {
    if (rlpData.length == 0) return null
    rlpData
  }
}
