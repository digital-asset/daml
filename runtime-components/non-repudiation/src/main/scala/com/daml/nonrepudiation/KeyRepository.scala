// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.{MessageDigest, PublicKey}

import com.google.common.io.BaseEncoding

import scala.collection.concurrent.TrieMap

object KeyRepository {

  trait Read {
    def get(fingerprint: String): Option[PublicKey]
  }

  trait Write {
    def put(key: PublicKey): String
  }

  private def hash(key: PublicKey): String = {
    val sha256: MessageDigest = MessageDigest.getInstance("SHA-256")
    val fingerprint: Array[Byte] = sha256.digest(key.getEncoded)
    BaseEncoding.base64.encode(fingerprint)
  }

  final class InMemory(keys: PublicKey*) extends KeyRepository {

    private val map: TrieMap[String, PublicKey] = TrieMap(keys.map(key => hash(key) -> key): _*)

    override def get(fingerprint: String): Option[PublicKey] = map.get(fingerprint)

    override def put(key: PublicKey): String = {
      val fingerprint = hash(key)
      map.put(fingerprint, key)
      fingerprint
    }
  }

}

trait KeyRepository extends KeyRepository.Read with KeyRepository.Write
