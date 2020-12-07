// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.concurrent.atomic.AtomicInteger

import com.daml.resources.ResettableResourceOwner.Reset
import com.daml.resources.{Resource => AbstractResource}
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Span
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.ref.WeakReference

class ResettableResourceOwnerSpec extends AsyncWordSpec with AsyncTimeLimitedTests with Matchers {

  override def timeLimit: Span = 10.seconds

  private implicit val context: TestContext = new TestContext(executionContext)

  private type Resource[+T] = AbstractResource[TestContext, T]
  private val Resource = new ResourceFactories[TestContext]

  "resetting a resource" should {
    "reconstruct everything" in {
      val acquisitionCounter = new AtomicInteger(0)
      val releaseCounter = new AtomicInteger(0)
      val owner = ResettableResourceOwner(reset =>
        new AbstractResourceOwner[TestContext, (Reset, Int)] {
          override def acquire()(implicit context: TestContext): Resource[(Reset, Int)] =
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
        new AbstractResourceOwner[TestContext, (Reset, Int)] {
          override def acquire()(implicit context: TestContext): Resource[(Reset, Int)] =
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
      val resetOperationInputs = mutable.Buffer[Int]()
      val owner = ResettableResourceOwner[TestContext, (Reset, Int), Unit](
        initialValue = {},
        owner = reset =>
          _ =>
            new AbstractResourceOwner[TestContext, (Reset, Int)] {
              override def acquire()(implicit context: TestContext): Resource[(Reset, Int)] =
                Resource(Future.successful((reset, acquisitionCounter.incrementAndGet()))) { _ =>
                  releaseCounter.incrementAndGet()
                  Future.unit
                }
        },
        resetOperation = {
          case (_, counter) =>
            resetCounter.incrementAndGet()
            resetOperationInputs += counter
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
        resetOperationInputs should be(Seq(1))
      }
    }

    "pass reset operation values through" in {
      val owner = ResettableResourceOwner[TestContext, (Reset, Int), Int](
        initialValue = 0,
        owner = reset =>
          value =>
            new AbstractResourceOwner[TestContext, (Reset, Int)] {
              override def acquire()(implicit context: TestContext): Resource[(Reset, Int)] = {
                Resource.fromFuture(Future.successful((reset, value + 1)))
              }
        },
        resetOperation = {
          case (_, value) =>
            Future.successful(value + 1)
        }
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
          value should be(3)
        }
        _ <- resource.release()
      } yield succeed
    }

    "not hold on to old values" in {
      val acquisitions = mutable.Buffer[WeakReference[Object]]()
      val owner = ResettableResourceOwner(reset =>
        new AbstractResourceOwner[TestContext, (Reset, Object)] {
          override def acquire()(implicit context: TestContext): Resource[(Reset, Object)] = {
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
