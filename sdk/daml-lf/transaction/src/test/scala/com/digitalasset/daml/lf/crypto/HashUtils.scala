// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.daml.crypto.MessageDigestPrototype
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.crypto.HashUtils.HashTracer.StringHashTracer
import org.scalatest.matchers.should.Matchers

trait HashUtils { this: Matchers =>
  def assertStringTracer(stringHashTracer: StringHashTracer, hash: Hash) = {
    val messageDigest = MessageDigestPrototype.Sha256.newDigest
    messageDigest.update(stringHashTracer.asByteArray)
    Hash.assertFromByteArray(messageDigest.digest()) shouldBe hash
  }
}
