// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.concurrent.TrieMap

class TrieMapUtilTest extends AnyWordSpec with BaseTest {

  case class Error(key: Int, oldValue: String, newValue: String)

  "TrieMapUtil" should {

    "insert if absent" in {
      val map = TrieMap(1 -> "Foo", 2 -> "Bar")
      TrieMapUtil.insertIfAbsent(map, 3, "test", Error.apply _) shouldBe Right(())
    }

    "insert if idempotent" in {
      val map = TrieMap(1 -> "Foo", 2 -> "Bar")
      TrieMapUtil.insertIfAbsent(map, 2, "Bar", Error.apply _) shouldBe Right(())
    }

    "fail insert on different values " in {
      val map = TrieMap(1 -> "Foo", 2 -> "Bar")
      TrieMapUtil
        .insertIfAbsent(map, 2, "Something else", Error.apply _)
        .left
        .value shouldBe an[Error]
    }
  }
}
