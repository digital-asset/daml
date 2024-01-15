// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.testing

import com.daml.nonrepudiation.{SignedPayload, SignedPayloadRepository}
import com.daml.nonrepudiation.SignedPayloadRepository.KeyEncoder

import scala.collection.concurrent.TrieMap

final class SignedPayloads[Key: KeyEncoder] extends SignedPayloadRepository[Key] {

  private val map = TrieMap.empty[Key, Vector[SignedPayload]].withDefaultValue(Vector.empty)

  override def put(signedPayload: SignedPayload): Unit = {
    val key = keyEncoder.encode(signedPayload.payload)
    val _ = map.put(key, map(key) :+ signedPayload)
  }

  override def get(key: Key): Iterable[SignedPayload] =
    map(key)

  def size: Int = map.size

}
