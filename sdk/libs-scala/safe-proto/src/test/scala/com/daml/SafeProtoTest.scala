// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.google.protobuf._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SafeProtoTest extends AnyWordSpec with Inside with Matchers {

  private[this] def tuple(v: Value) =
    Value.newBuilder().setListValue(ListValue.newBuilder().addValues(v).addValues(v)).build()

  private[this] val value0 = Value.newBuilder().setStringValue("a" * 1024 * 1024 * 1024).build()
  assert(value0.getSerializedSize > 0)
  private[this] val value1 = tuple(value0)
  assert(value1.getSerializedSize < 0)
  private[this] val value2 = tuple(value1)
  assert(value2.getSerializedSize >= 0)

  "toByteString" should {

    "fail gracefully if the message to serialize exceeds 2GB" in {
      SafeProto.toByteString(value0) shouldBe a[Right[_, _]]
      SafeProto.toByteString(value1) shouldBe a[Left[_, _]]
      SafeProto.toByteString(value2) shouldBe a[Left[_, _]]
    }
  }

  "toByteArray" should {

    "fail gracefully if the message to serialize exceeds 2GB" in {
      SafeProto.toByteArray(value0) shouldBe a[Right[_, _]]
      SafeProto.toByteArray(value1) shouldBe a[Left[_, _]]
      SafeProto.toByteArray(value2) shouldBe a[Left[_, _]]
    }
  }

}
