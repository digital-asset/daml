// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.serialization.DeterministicEncoding
import com.google.protobuf.ByteString

/** A wrapper around a real hash with a testing-only hash purpose and convenience methods */
object TestHash extends HashOps {

  val testHashPurpose: HashPurpose = HashPurpose(-1, "testing")

  override def defaultHashAlgorithm: HashAlgorithm = HashAlgorithm.Sha256

  def digest(bytes: ByteString): Hash =
    digest(testHashPurpose, bytes, defaultHashAlgorithm)

  def digest(bytesAsString: String): Hash = digest(ByteString.copyFromUtf8(bytesAsString))

  def digest(bytesAsInt: Int): Hash = digest(DeterministicEncoding.encodeInt(bytesAsInt))

  def build: HashBuilder = build(testHashPurpose)

}
