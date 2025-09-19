// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto.rlp

/**
 * Wrapper class for decoded elements from an RLP encoded byte array.
 *
 * @author Roman Mandeleil
 * @since 01.04.2014
 */
trait RLPElement extends Serializable {
  def getRLPData: Array[Byte]
}
