// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import java.util.concurrent.atomic.AtomicBoolean

import com.digitalasset.platform.resources.ResourceOwnerSpec._
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.{ExecutionContext, Future}

class ResourceOwnerSpec extends AsyncWordSpec with Matchers {
  "a resource owner" should {
    "be closeable" in {
      val owner = new TestResourceOwner(42)
      owner.isOpen should be(false)

      val open = for {
        value <- owner.open()
      } yield {
        owner.isOpen should be(true)
        value should be(42)
      }

      for {
        _ <- open.close()
      } yield {
        owner.isOpen should be(false)
      }
    }

    "close all sub-resources" in {
      val ownerA = new TestResourceOwner(1)
      val ownerB = new TestResourceOwner("two")

      val open = for {
        _ <- ownerA.open()
        _ <- ownerB.open()
      } yield {
        ownerA.isOpen should be(true)
        ownerB.isOpen should be(true)
        ()
      }

      for {
        _ <- open.close()
      } yield {
        ownerA.isOpen should be(false)
        ownerB.isOpen should be(false)
      }
    }

    "handles failure gracefully" in {
      val owner = new FailingResourceOwner[String]()

      val open = owner.open()

      for {
        throwable <- open.asFuture.failed
      } yield {
        throwable should be(a[FailingResourceFailedToOpen])
      }
    }

    "on failure, close any open sub-resources" in {
      val ownerA = new TestResourceOwner(1)
      val ownerB = new TestResourceOwner(2)
      val ownerC = new FailingResourceOwner[Int]()

      val open = for {
        openA <- ownerA.open()
        openB <- ownerB.open()
        openC <- ownerC.open()
      } yield openA + openB + openC

      for {
        throwable <- open.asFuture.failed
      } yield {
        throwable should be(a[FailingResourceFailedToOpen])
        ownerA.isOpen should be(false)
        ownerB.isOpen should be(false)
      }
    }

    "on failure mid-way, close any open sub-resources" in {
      val ownerA = new TestResourceOwner(5)
      val ownerB = new TestResourceOwner(6)
      val ownerC = new FailingResourceOwner[Int]()
      val ownerD = new TestResourceOwner(8)

      val open = for {
        openA <- ownerA.open()
        openB <- ownerB.open()
        openC <- ownerC.open()
        openD <- ownerD.open()
      } yield openA + openB + openC + openD

      for {
        throwable <- open.asFuture.failed
      } yield {
        throwable should be(a[FailingResourceFailedToOpen])
        ownerA.isOpen should be(false)
        ownerB.isOpen should be(false)
        ownerD.isOpen should be(false)
      }
    }

    "on filter, close any open sub-resources" in {
      val ownerA = new TestResourceOwner(99)
      val ownerB = new TestResourceOwner(100)

      val open = for {
        openA <- ownerA.open()
        if false
        openB <- ownerB.open()
      } yield openA + openB

      for {
        throwable <- open.asFuture.failed
      } yield {
        throwable should be(a[ResourceAcquisitionFilterException])
        ownerA.isOpen should be(false)
        ownerB.isOpen should be(false)
      }
    }
  }
}

object ResourceOwnerSpec {
  final class TestResourceOwner[T](value: T) extends ResourceOwner[T] {
    private val opened = new AtomicBoolean()

    def isOpen: Boolean = opened.get

    def open()(implicit executionContext: ExecutionContext): Open[T] = {
      if (!opened.compareAndSet(false, true)) {
        throw new TriedToOpenTwice
      }
      new Open[T] {
        override val asFuture: Future[T] =
          Future.successful(TestResourceOwner.this.value)

        override def close(): Future[Unit] =
          if (opened.compareAndSet(true, false))
            Future.successful(())
          else
            Future.failed(new TriedToCloseTwice)
      }
    }
  }

  final class FailingResourceOwner[T] extends ResourceOwner[T] {
    override def open()(implicit executionContext: ExecutionContext): Open[T] =
      new Open[T] {
        override val asFuture: Future[T] =
          Future.failed(new FailingResourceFailedToOpen)

        override def close(): Future[Unit] =
          Future.successful(())
      }
  }

  final class TriedToOpenTwice extends Exception("Tried to open twice.")

  final class TriedToCloseTwice extends Exception("Tried to close twice.")

  final class FailingResourceFailedToOpen extends Exception("Something broke!")
}
