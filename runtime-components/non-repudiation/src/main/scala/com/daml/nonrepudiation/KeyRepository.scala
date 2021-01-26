// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.PublicKey

import scala.collection.concurrent.TrieMap
import scala.util.Try

object KeyRepository {

  type Fingerprint <: String

  object Fingerprint {

    def assertFromString(string: String): Fingerprint =
      string.asInstanceOf[Fingerprint]

    def fromString(string: String): Try[Fingerprint] =
      Try(assertFromString(string))

    def compute(key: PublicKey): Fingerprint =
      assertFromString(Base64Fingerprint.compute(key))

  }

  trait Read {
    def get(fingerprint: Fingerprint): Option[PublicKey]
  }

  trait Write {
    def put(key: PublicKey): Fingerprint
  }

  final class InMemory(keys: PublicKey*) extends KeyRepository {

    private val map: TrieMap[Fingerprint, PublicKey] = TrieMap(
      keys.map(key => Fingerprint.compute(key) -> key): _*
    )

    override def get(fingerprint: Fingerprint): Option[PublicKey] = map.get(fingerprint)

    override def put(key: PublicKey): Fingerprint = {
      val fingerprint = Fingerprint.compute(key)
      map.put(fingerprint, key)
      fingerprint
    }
  }

}

trait KeyRepository extends KeyRepository.Read with KeyRepository.Write
