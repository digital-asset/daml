// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Id
import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable

class MonadUtilTest extends AnyWordSpec with BaseTest {
  "sequential traverse" should {

    "return results in the order of the original seq" in {
      val xs = List(1, 2, 3)
      val result = MonadUtil.sequentialTraverse(xs)(x => x: Id[Int])
      result shouldEqual xs
    }

    "perform processing in the order of the original seq" in {
      val xs = List(1, 2, 3)
      val seen = mutable.ArrayDeque[Int]()
      MonadUtil.sequentialTraverse(xs) { x =>
        seen += x // Appends the seen element to the list
        x: Id[Int]
      }
      seen.toList shouldEqual xs
    }
  }
}
