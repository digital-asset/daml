// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml

import com.google.protobuf._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

// The test is designed to need less than 1Gb of Heap
class SafeProtoTest extends AnyWordSpec with Inside with Matchers {

  private[this] def tuple(v: Value) =
    Value.newBuilder().setListValue(ListValue.newBuilder().addValues(v).addValues(v)).build()

  // We maximize structural sharing to limit the JVM heap memory usage.
  // This allows us to create objects that would serialize to massive sizes (e.g. > 512MB or 4GB)
  // without actually consuming that much RAM.
  private[this] lazy val values =
    LazyList.iterate(Value.newBuilder().setStringValue("a" * 1024).build())(tuple)

  // values(19), and  values(20) are technically serializable, but their serialization requires over 500 MB of memory.
  // We skip it to avoid excessive memory pressure on CI agents.
  private[this] val value18 = values(18) // serialization needs a bit more than 256MB
  assert(value18.getSerializedSize > 0)
  private[this] val value21 = values(21) // serialization would need a bit more than 2GB
  assert(value21.getSerializedSize < 0)
  private[this] val value22 = values(22) // serialization would need a bit more than 4GB
  assert(value22.getSerializedSize >= 0)

  "toByteString" should {

    "fail gracefully if the message to serialize exceeds 2GB" in {
      SafeProto.toByteString(value18) shouldBe a[Right[?, ?]]
      SafeProto.toByteString(value21) shouldBe a[Left[?, ?]]
      SafeProto.toByteString(value22) shouldBe a[Left[?, ?]]
    }
  }

  "toByteArray" should {

    "fail gracefully if the message to serialize exceeds 2GB" in {
      SafeProto.toByteArray(value18) shouldBe a[Right[?, ?]]
      SafeProto.toByteArray(value21) shouldBe a[Left[?, ?]]
      SafeProto.toByteArray(value22) shouldBe a[Left[?, ?]]
    }
  }

}
