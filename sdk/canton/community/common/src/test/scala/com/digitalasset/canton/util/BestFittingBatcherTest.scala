// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.util.BestFittingBatcherTest.TestItems
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.IterableOps

class BestFittingBatcherTest extends AnyWordSpec with BaseTest {
  "BestFitter" should {
    "pack items optimally" in {
      val maxSize = PositiveInt.tryCreate(10)
      val fitter =
        new BestFittingBatcher[TestItems](maxBatchSize = maxSize)

      fitter.poll() shouldBe None

      fitter.add(TestItems(3)) shouldBe true
      fitter.poll() shouldBe Some(Vector(TestItems(3)))

      (1 to 3).foreach { _ =>
        fitter.add(TestItems(4)) shouldBe true
      }

      fitter.poll() shouldBe Some(Vector.fill(2)(TestItems(4)))
      fitter.poll() shouldBe Some(Vector(TestItems(4)))
      fitter.poll() shouldBe None

      (1 to 3).foreach { _ =>
        fitter.add(TestItems(5)) shouldBe true
      }

      fitter.poll() shouldBe Some(Vector.fill(2)(TestItems(5)))
      fitter.poll() shouldBe Some(Vector(TestItems(5)))
      fitter.poll() shouldBe None

      (6.to(4, -1)).foreach { size =>
        fitter.add(TestItems(size)) shouldBe true
      }

      fitter.poll() shouldBe Some(Vector(TestItems(6), TestItems(4)))
      fitter.poll() shouldBe Some(Vector(TestItems(5)))
      fitter.poll() shouldBe None

      (4 to 6).foreach { size =>
        fitter.add(TestItems(size)) shouldBe true
      }

      fitter.poll() shouldBe Some(Vector(TestItems(4), TestItems(5)))
      fitter.poll() shouldBe Some(Vector(TestItems(6)))
      fitter.poll() shouldBe None

      fitter.add(TestItems(maxSize.value + 1)) shouldBe false
    }
  }
}

object BestFittingBatcherTest {

  final case class TestItems(override val size: PositiveInt) extends BestFittingBatcher.Sized {
    val data: Seq[Unit] = Vector.fill(size.value)(())
    override def sizeIs: IterableOps.SizeCompareOps = data.sizeIs
  }
  object TestItems {
    def apply(size: Int): TestItems = TestItems(PositiveInt.tryCreate(size))
  }
}
