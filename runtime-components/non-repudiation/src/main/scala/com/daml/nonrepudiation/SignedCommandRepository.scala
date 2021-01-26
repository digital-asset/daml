// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import com.google.common.io.BaseEncoding

import scala.collection.concurrent.TrieMap

object SignedCommandRepository {

  trait Read {
    def get(command: Array[Byte]): Option[String]
  }

  trait Write {
    def put(command: Array[Byte], signature: String): Unit
  }

  final class InMemory() extends SignedCommandRepository {
    private val map: TrieMap[String, String] = TrieMap.empty

    override def put(command: Array[Byte], signature: String): Unit = {
      val _ = map.put(BaseEncoding.base64().encode(command), signature)
    }

    override def get(command: Array[Byte]): Option[String] =
      map.get(BaseEncoding.base64().encode(command))
  }

}

trait SignedCommandRepository
    extends SignedCommandRepository.Read
    with SignedCommandRepository.Write
