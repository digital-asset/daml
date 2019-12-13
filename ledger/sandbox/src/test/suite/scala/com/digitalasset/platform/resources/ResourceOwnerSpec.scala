// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import java.util.concurrent.atomic.AtomicBoolean

import com.digitalasset.platform.resources.ResourceOwnerSpec._
import org.scalatest.{Matchers, WordSpec}

class ResourceOwnerSpec extends WordSpec with Matchers {
  "a resource owner" should {
    "be closeable" in {
      val owner = new TestResourceOwner(42)
      owner.isOpen should be(false)

      val opened = owner.open()
      owner.isOpen should be(true)
      opened.value should be(42)

      opened.close()
      owner.isOpen should be(false)
    }

    "close all sub-resources" in {
      val ownerA = new TestResourceOwner(1)
      val ownerB = new TestResourceOwner("two")

      val open = for {
        openA <- ownerA.open()
        openB <- ownerB.open()
      } yield (openA, openB)
      ownerA.isOpen should be(true)
      ownerB.isOpen should be(true)

      open.close()
      ownerA.isOpen should be(false)
      ownerB.isOpen should be(false)
    }

    "on failure, closes any open sub-resources" in {
      val ownerA = new TestResourceOwner(1)
      val ownerB = new TestResourceOwner(2)
      val ownerC = new FailingResourceOwner[Int]()

      an[IllegalStateException] should be thrownBy (
        for {
          openA <- ownerA.open()
          openB <- ownerB.open()
          openC <- ownerC.open()
        } yield openA + openB + openC
      )

      ownerA.isOpen should be(false)
      ownerB.isOpen should be(false)
    }

    "on failure mid-way, closes any open sub-resources" in {
      val ownerA = new TestResourceOwner(1)
      val ownerB = new TestResourceOwner(2)
      val ownerC = new FailingResourceOwner[Int]()
      val ownerD = new TestResourceOwner(4)

      an[IllegalStateException] should be thrownBy (
        for {
          openA <- ownerA.open()
          openB <- ownerB.open()
          openC <- ownerC.open()
          openD <- ownerD.open()
        } yield openA + openB + openC + openD
      )

      ownerA.isOpen should be(false)
      ownerB.isOpen should be(false)
      ownerD.isOpen should be(false)
    }

    "on filter, closes any open sub-resources" in {
      val ownerA = new TestResourceOwner(1)
      val ownerB = new TestResourceOwner(2)

      a[ResourceAcquisitionFilterException] should be thrownBy (
        for {
          openA <- ownerA.open()
          if false
          openB <- ownerB.open()
        } yield openA + openB
      )

      ownerA.isOpen should be(false)
      ownerB.isOpen should be(false)
    }
  }
}

object ResourceOwnerSpec {
  final class TestResourceOwner[T](value: T) extends ResourceOwner[T] {
    private val opened = new AtomicBoolean()

    def isOpen: Boolean = opened.get

    def open(): Open[T] = {
      if (!opened.compareAndSet(false, true)) {
        throw new IllegalStateException("Tried to open twice.")
      }
      new Open[T] {
        override val value: T = TestResourceOwner.this.value

        override def close(): Unit = {
          if (!opened.compareAndSet(true, false)) {
            throw new IllegalStateException("Tried to close twice.")
          }
        }
      }
    }
  }

  final class FailingResourceOwner[T] extends ResourceOwner[T] {
    override def open(): Open[T] = throw new IllegalStateException("Something broke!")
  }
}
