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
      owner.hasBeenAcquired should be(false)

      val resource = for {
        value <- owner.acquire()
      } yield {
        owner.hasBeenAcquired should be(true)
        value should be(42)
      }

      for {
        _ <- resource.release()
      } yield {
        owner.hasBeenAcquired should be(false)
      }
    }

    "close all sub-resources" in {
      val ownerA = new TestResourceOwner(1)
      val ownerB = new TestResourceOwner("two")

      val resource = for {
        _ <- ownerA.acquire()
        _ <- ownerB.acquire()
      } yield {
        ownerA.hasBeenAcquired should be(true)
        ownerB.hasBeenAcquired should be(true)
        ()
      }

      for {
        _ <- resource.release()
      } yield {
        ownerA.hasBeenAcquired should be(false)
        ownerB.hasBeenAcquired should be(false)
      }
    }

    "handles failure gracefully" in {
      val owner = new FailingResourceOwner[String]()

      val resource = owner.acquire()

      for {
        throwable <- resource.asFuture.failed
      } yield {
        throwable should be(a[FailingResourceFailedToOpen])
      }
    }

    "on failure, release any acquired sub-resources" in {
      val ownerA = new TestResourceOwner(1)
      val ownerB = new TestResourceOwner(2)
      val ownerC = new FailingResourceOwner[Int]()

      val resource = for {
        resourceA <- ownerA.acquire()
        resourceB <- ownerB.acquire()
        resourceC <- ownerC.acquire()
      } yield resourceA + resourceB + resourceC

      for {
        throwable <- resource.asFuture.failed
      } yield {
        throwable should be(a[FailingResourceFailedToOpen])
        ownerA.hasBeenAcquired should be(false)
        ownerB.hasBeenAcquired should be(false)
      }
    }

    "on failure mid-way, release any acquired sub-resources" in {
      val ownerA = new TestResourceOwner(5)
      val ownerB = new TestResourceOwner(6)
      val ownerC = new FailingResourceOwner[Int]()
      val ownerD = new TestResourceOwner(8)

      val resource = for {
        resourceA <- ownerA.acquire()
        resourceB <- ownerB.acquire()
        resourceC <- ownerC.acquire()
        resourceD <- ownerD.acquire()
      } yield resourceA + resourceB + resourceC + resourceD

      for {
        throwable <- resource.asFuture.failed
      } yield {
        throwable should be(a[FailingResourceFailedToOpen])
        ownerA.hasBeenAcquired should be(false)
        ownerB.hasBeenAcquired should be(false)
        ownerD.hasBeenAcquired should be(false)
      }
    }

    "on filter, release any acquired sub-resources" in {
      val ownerA = new TestResourceOwner(99)
      val ownerB = new TestResourceOwner(100)

      val resource = for {
        resourceA <- ownerA.acquire()
        if false
        resourceB <- ownerB.acquire()
      } yield resourceA + resourceB

      for {
        throwable <- resource.asFuture.failed
      } yield {
        throwable should be(a[ResourceAcquisitionFilterException])
        ownerA.hasBeenAcquired should be(false)
        ownerB.hasBeenAcquired should be(false)
      }
    }
  }
}

object ResourceOwnerSpec {
  final class TestResourceOwner[T](value: T) extends ResourceOwner[T] {
    private val acquired = new AtomicBoolean(false)

    def hasBeenAcquired: Boolean = acquired.get

    def acquire()(implicit _executionContext: ExecutionContext): Resource[T] = {
      if (!acquired.compareAndSet(false, true)) {
        throw new TriedToAcquireTwice
      }
      new Resource[T] {
        override protected val executionContext: ExecutionContext = _executionContext

        override protected val future: Future[T] =
          Future.successful(TestResourceOwner.this.value)

        override def release(): Future[Unit] =
          if (acquired.compareAndSet(true, false))
            Future.successful(())
          else
            Future.failed(new TriedToReleaseTwice)
      }
    }
  }

  final class FailingResourceOwner[T] extends ResourceOwner[T] {
    override def acquire()(implicit _executionContext: ExecutionContext): Resource[T] =
      new Resource[T] {
        private def closedAlready = new AtomicBoolean(false)

        override protected val executionContext: ExecutionContext = _executionContext

        override protected val future: Future[T] =
          Future.failed(new FailingResourceFailedToOpen)

        override def release(): Future[Unit] =
          if (closedAlready.compareAndSet(false, true))
            Future.successful(())
          else
            Future.failed(new TriedToReleaseTwice)
      }
  }

  final class TriedToAcquireTwice extends Exception("Tried to acquire twice.")

  final class TriedToReleaseTwice extends Exception("Tried to release twice.")

  final class FailingResourceFailedToOpen extends Exception("Something broke!")
}
