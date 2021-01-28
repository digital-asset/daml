// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.PublicKey

import com.google.common.io.BaseEncoding

import scala.collection.concurrent.TrieMap

object KeyRepository {

  object Fingerprint {
    def wrap(bytes: Array[Byte]): Fingerprint = new Fingerprint(bytes)
    def wrap(key: PublicKey): Fingerprint = wrap(Fingerprints.compute(key))
  }

  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  final class Fingerprint(val bytes: Array[Byte]) extends AnyVal {
    def base64: String = BaseEncoding.base64().encode(bytes)
  }

  trait Read {
    def get(fingerprint: Fingerprint): Option[PublicKey]
  }

  trait Write {
    def put(key: PublicKey): Fingerprint
  }

  final class InMemory(keys: PublicKey*) extends KeyRepository {

    private val map: TrieMap[String, PublicKey] = TrieMap(
      keys.map(key => BaseEncoding.base64().encode(Fingerprints.compute(key)) -> key): _*
    )

    override def get(fingerprint: Fingerprint): Option[PublicKey] =
      map.get(fingerprint.base64)

    override def put(key: PublicKey): Fingerprint = {
      val fingerprint = Fingerprint.wrap(key)
      map.put(fingerprint.base64, key)
      fingerprint
    }
  }

}

trait KeyRepository extends KeyRepository.Read with KeyRepository.Write
