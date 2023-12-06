// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class SingleUseCellTest extends AnyWordSpec with BaseTest {

  def mk[A](): SingleUseCell[A] = new SingleUseCell[A]

  "SingleUseCell" should {

    "initially be empty" in {
      val cell = mk[Int]()
      assert(cell.isEmpty)
      assert(cell.get.isEmpty)
    }

    "return the written value" in {
      val cell = mk[Int]()
      assert(cell.putIfAbsent(7).isEmpty)
      assert(cell.get.contains(7))
    }

    "not overwrite values" in {
      val cell = mk[Int]()
      cell.putIfAbsent(12)
      assert(cell.putIfAbsent(1).contains(12))
      assert(cell.get.contains(12))
    }
  }
}
