// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import java.util.concurrent.atomic.AtomicInteger

import com.digitalasset.resources.ResettableResourceOwner.Reset
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.ref.WeakReference

class ResettableResourceOwnerSpec extends AsyncWordSpec with AsyncTimeLimitedTests with Matchers {

  override def timeLimit: Span = 10.seconds

  "resetting a resource" should {
    "reconstruct everything" in {
      val acquisitionCounter = new AtomicInteger(0)
      val releaseCounter = new AtomicInteger(0)
      val owner = ResettableResourceOwner(reset =>
        new ResourceOwner[(Reset, Int)] {
          override def acquire()(
              implicit executionContext: ExecutionContext
          ): Resource[(Reset, Int)] =
            Resource(Future.successful((reset, acquisitionCounter.incrementAndGet()))) { _ =>
              releaseCounter.incrementAndGet()
              Future.unit
            }
      })

      withClue("before acquisition, ") {
        acquisitionCounter.get() should be(0)
        releaseCounter.get() should be(0)
      }

      val resource = owner.acquire()

      for {
        (reset, value) <- resource.asFuture
        _ = withClue("after first acquisition, ") {
          value should be(1)
          acquisitionCounter.get() should be(1)
          releaseCounter.get() should be(0)
        }
        _ <- reset()
        (reset, value) <- resource.asFuture
        _ = withClue("after first reset, ") {
          value should be(2)
          acquisitionCounter.get() should be(2)
          releaseCounter.get() should be(1)
        }
        _ <- reset()
        (_, value) <- resource.asFuture
        _ = withClue("after second reset, ") {
          value should be(3)
          acquisitionCounter.get() should be(3)
          releaseCounter.get() should be(2)
        }
        _ <- resource.release()
      } yield {
        withClue("after release, ") {
          acquisitionCounter.get() should be(3)
          releaseCounter.get() should be(3)
        }
      }
    }

    "avoid resetting twice concurrently" in {
      val acquisitionCounter = new AtomicInteger(0)
      val releaseCounter = new AtomicInteger(0)
      val owner = ResettableResourceOwner(reset =>
        new ResourceOwner[(Reset, Int)] {
          override def acquire()(
              implicit executionContext: ExecutionContext
          ): Resource[(Reset, Int)] =
            Resource(Future.successful((reset, acquisitionCounter.incrementAndGet()))) { _ =>
              releaseCounter.incrementAndGet()
              Future.unit
            }
      })

      val resource = owner.acquire()
      for {
        (reset, _) <- resource.asFuture
        _ <- Future.sequence((1 to 10).map(_ => reset()))
        _ <- resource.release()
      } yield {
        acquisitionCounter.get() should be(2)
        releaseCounter.get() should be(2)
      }
    }

    "run an extra operation if specified" in {
      val acquisitionCounter = new AtomicInteger(0)
      val releaseCounter = new AtomicInteger(0)
      val resetCounter = new AtomicInteger(0)
      val owner = ResettableResourceOwner(
        reset =>
          new ResourceOwner[(Reset, Int)] {
            override def acquire()(
                implicit executionContext: ExecutionContext
            ): Resource[(Reset, Int)] =
              Resource(Future.successful((reset, acquisitionCounter.incrementAndGet()))) { _ =>
                releaseCounter.incrementAndGet()
                Future.unit
              }
        },
        resetOperation = () => {
          resetCounter.incrementAndGet()
          Future.unit
        }
      )

      val resource = owner.acquire()
      withClue("after first acquisition, ") {
        acquisitionCounter.get() should be(1)
        releaseCounter.get() should be(0)
        resetCounter.get() should be(0)
      }

      for {
        (reset, _) <- resource.asFuture
        _ <- reset()
        _ = withClue("after reset, ") {
          acquisitionCounter.get() should be(2)
          releaseCounter.get() should be(1)
          resetCounter.get() should be(1)
        }
        _ <- resource.release()
      } yield {
        acquisitionCounter.get() should be(2)
        releaseCounter.get() should be(2)
        resetCounter.get() should be(1)
      }
    }

    "pass reset operation values through" in {
      val resetPromise: Promise[Unit] = Promise[Unit]()
      val owner = ResettableResourceOwner[(Reset, Int), Int](
        reset =>
          value =>
            new ResourceOwner[(Reset, Int)] {
              override def acquire()(
                  implicit executionContext: ExecutionContext
              ): Resource[(Reset, Int)] = {
                Resource.fromFuture(Future.successful((reset, value + 1)))
              }
        },
        initialValue = 0,
        resetOperation = () => Future.successful(7)
      )

      val resource = owner.acquire()

      for {
        (reset, value) <- resource.asFuture
        _ = withClue("after first acquisition, ") {
          value should be(1)
        }
        _ <- reset()
        (_, value) <- resource.asFuture
        _ = withClue("after reset, ") {
          value should be(8)
        }
        _ <- resource.release()
      } yield succeed
    }

    "not hold on to old values" in {
      var acquisitions = mutable.Buffer[WeakReference[Object]]()
      val resetPromise: Promise[Unit] = Promise[Unit]()
      val owner = ResettableResourceOwner(reset =>
        new ResourceOwner[(Reset, Object)] {
          override def acquire()(
              implicit executionContext: ExecutionContext
          ): Resource[(Reset, Object)] = {
            val obj = new Object
            acquisitions += new WeakReference(obj)
            Resource.fromFuture(Future.successful((reset, obj)))
          }
      })

      val resource = owner.acquire()
      System.gc()
      acquisitions should have size 1
      acquisitions.filter(_.get.isDefined) should have size 1

      for {
        (reset, _) <- resource.asFuture
        _ <- reset()
        _ = withClue("after reset, ") {
          System.gc()
          acquisitions should have size 2
          acquisitions.filter(_.get.isDefined) should have size 1
        }
        _ <- resource.release()
      } yield {
        System.gc()
        acquisitions should have size 2
        acquisitions.filter(_.get.isDefined) should have size 0
      }
    }
  }
}
