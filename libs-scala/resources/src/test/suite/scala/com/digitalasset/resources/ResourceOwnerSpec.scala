// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.{Executors, RejectedExecutionException}
import java.util.{Timer, TimerTask}

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.{Done, NotUsed}
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class ResourceOwnerSpec extends AsyncWordSpec with Matchers {
  "a resource owner" should {
    "acquire and release a resource" in {
      val owner = TestResourceOwner(42)
      withClue("after construction,") {
        owner.hasBeenAcquired should be(false)
      }

      val resource = for {
        value <- owner.acquire()
      } yield {
        withClue("after acquiring,") {
          owner.hasBeenAcquired should be(true)
          value should be(42)
        }
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        withClue("after releasing,") {
          owner.hasBeenAcquired should be(false)
        }
      }
    }

    "release all sub-resources when released" in {
      val ownerA = TestResourceOwner(1)
      val ownerB = TestResourceOwner("two")

      val resource = for {
        _ <- ownerA.acquire()
        _ <- ownerB.acquire()
      } yield {
        withClue("after acquiring,") {
          ownerA.hasBeenAcquired should be(true)
          ownerB.hasBeenAcquired should be(true)
        }
        ()
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        withClue("after releasing,") {
          ownerA.hasBeenAcquired should be(false)
          ownerB.hasBeenAcquired should be(false)
        }
      }
    }

    "treat releases idempotently, only releasing once regardless of the number of calls" in {
      val ownerA = TestResourceOwner(7)
      val ownerB = TestResourceOwner("eight")

      val resource = for {
        _ <- ownerA.acquire()
        _ <- ownerB.acquire()
      } yield ()

      for {
        _ <- resource.asFuture
        _ <- resource.release()
        // if `TestResourceOwner`'s release function is called twice, it'll fail
        _ <- resource.release()
      } yield {
        ownerA.hasBeenAcquired should be(false)
        ownerB.hasBeenAcquired should be(false)
      }
    }

    "never complete a second release before the first one is complete" in {
      val ownerA = TestResourceOwner("nine")
      val ownerB = DelayedReleaseResourceOwner("ten", releaseDelay = 100.milliseconds)

      val resource = for {
        _ <- ownerA.acquire()
        _ <- ownerB.acquire()
      } yield ()

      val releaseCapture = Promise[Unit]()
      resource.release().onComplete(releaseCapture.complete)

      val testResult = for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        ownerA.hasBeenAcquired should be(false)
        ownerB.hasBeenAcquired should be(false)
      }

      for {
        result <- testResult
        _ <- releaseCapture.future
      } yield result
    }

    "handles failure gracefully" in {
      val owner = FailingResourceOwner[String]()

      val resource = owner.acquire()

      for {
        throwable <- resource.asFuture.failed
        _ <- resource.release()
      } yield {
        throwable should be(a[FailingResourceOwner.FailingResourceFailedToOpen])
      }
    }

    "on failure, release any acquired sub-resources" in {
      val ownerA = TestResourceOwner(1)
      val ownerB = TestResourceOwner(2)
      val ownerC = FailingResourceOwner[Int]()

      val resource = for {
        resourceA <- ownerA.acquire()
        resourceB <- ownerB.acquire()
        resourceC <- ownerC.acquire()
      } yield resourceA + resourceB + resourceC

      for {
        throwable <- resource.asFuture.failed
      } yield {
        throwable should be(a[FailingResourceOwner.FailingResourceFailedToOpen])
        ownerA.hasBeenAcquired should be(false)
        ownerB.hasBeenAcquired should be(false)
      }
    }

    "on failure mid-way, release any acquired sub-resources" in {
      val ownerA = TestResourceOwner(5)
      val ownerB = TestResourceOwner(6)
      val ownerC = FailingResourceOwner[Int]()
      val ownerD = TestResourceOwner(8)

      val resource = for {
        resourceA <- ownerA.acquire()
        resourceB <- ownerB.acquire()
        resourceC <- ownerC.acquire()
        resourceD <- ownerD.acquire()
      } yield resourceA + resourceB + resourceC + resourceD

      for {
        throwable <- resource.asFuture.failed
      } yield {
        throwable should be(a[FailingResourceOwner.FailingResourceFailedToOpen])
        ownerA.hasBeenAcquired should be(false)
        ownerB.hasBeenAcquired should be(false)
        ownerD.hasBeenAcquired should be(false)
      }
    }

    "on filter, release any acquired sub-resources" in {
      val ownerA = TestResourceOwner(99)
      val ownerB = TestResourceOwner(100)

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

    "flatten nested resources" in {
      val innerResourceOwner = TestResourceOwner(72)
      val outerResourceOwner = TestResourceOwner(innerResourceOwner.acquire())
      val outerResource = outerResourceOwner.acquire()

      val resource = for {
        resource <- outerResource.flatten
      } yield {
        withClue("after flattening,") {
          resource should be(72)
          innerResourceOwner.hasBeenAcquired should be(true)
          outerResourceOwner.hasBeenAcquired should be(true)
        }
        resource
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        withClue("after releasing,") {
          innerResourceOwner.hasBeenAcquired should be(false)
          outerResourceOwner.hasBeenAcquired should be(false)
        }
      }
    }

    "transform success into another resource" in {
      val ownerA = TestResourceOwner(1)
      val ownerB = TestResourceOwner(2)

      val resource = for {
        a <- ownerA.acquire()
        b <- ownerB.acquire()
      } yield {
        withClue("after acquiring,") {
          ownerA.hasBeenAcquired should be(true)
          ownerB.hasBeenAcquired should be(true)
        }
        a + b
      }

      val transformedResource = resource.transformWith {
        case Success(value) => Resource.successful(value + 1)
        case Failure(exception) =>
          Resource.failed[Int](new IllegalStateException("Unexpected failure.", exception))
      }

      for {
        value <- transformedResource.asFuture
        _ <- transformedResource.release()
      } yield {
        withClue("after releasing,") {
          value should be(4)
          ownerA.hasBeenAcquired should be(false)
          ownerB.hasBeenAcquired should be(false)
        }
      }
    }

    "transform failure into another resource" in {
      val ownerA = TestResourceOwner(1)
      val ownerB = FailingResourceOwner[Int]()

      val resource = for {
        a <- ownerA.acquire()
        b <- ownerB.acquire()
      } yield {
        withClue("after acquiring,") {
          ownerA.hasBeenAcquired should be(true)
        }
        a + b
      }

      val transformedResource = resource.transformWith {
        case Success(_) => Resource.failed[String](new IllegalStateException("Unexpected success."))
        case Failure(_) => Resource.successful("something")
      }

      for {
        value <- transformedResource.asFuture
        _ <- transformedResource.release()
      } yield {
        withClue("after releasing,") {
          value should be("something")
          ownerA.hasBeenAcquired should be(false)
        }
      }
    }

    "map and flatMap just like a Resource" in {
      val ownerA = TestResourceOwner(1)
      val ownerB = TestResourceOwner(2)
      val ownerC = TestResourceOwner(3)

      val owner = for {
        resourceA <- ownerA
        resourceB <- ownerB
        resourceC <- ownerC
      } yield resourceA + resourceB + resourceC

      val resource = for {
        value <- owner.acquire()
      } yield {
        withClue("after acquiring,") {
          value should be(6)
          ownerA.hasBeenAcquired should be(true)
          ownerB.hasBeenAcquired should be(true)
          ownerC.hasBeenAcquired should be(true)
        }
        value
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        withClue("after releasing,") {
          ownerA.hasBeenAcquired should be(false)
          ownerB.hasBeenAcquired should be(false)
          ownerC.hasBeenAcquired should be(false)
        }
      }
    }
  }

  "a pure value" should {
    "convert to a ResourceOwner" in {
      val resource = for {
        value <- ResourceOwner.successful("Hello!").acquire()
      } yield {
        value should be("Hello!")
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        succeed
      }
    }
  }

  "a throwable" should {
    "convert to a failed ResourceOwner" in {
      object ExampleThrowable extends Exception("Example throwable.")

      val resource = ResourceOwner.failed(ExampleThrowable).acquire()

      for {
        throwable <- resource.asFuture.failed
        _ <- resource.release()
      } yield {
        throwable should be(ExampleThrowable)
      }
    }
  }

  "a function returning a Try" should {
    "convert to a ResourceOwner" in {
      val resource = for {
        value <- ResourceOwner.forTry(() => Success(49)).acquire()
      } yield {
        value should be(49)
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        succeed
      }
    }
  }

  "a function returning a Future" should {
    "convert to a ResourceOwner" in {
      val resource = for {
        value <- ResourceOwner.forFuture(() => Future.successful(54)).acquire()
      } yield {
        value should be(54)
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        succeed
      }
    }
  }

  "a function returning a CompletionStage" should {
    "convert to a ResourceOwner" in {
      val resource = for {
        value <- ResourceOwner
          .forCompletionStage(() => completedFuture(63))
          .acquire()
      } yield {
        value should be(63)
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        succeed
      }
    }
  }

  "a function returning an AutoCloseable" should {
    "convert to a ResourceOwner" in {
      val newCloseable = new MockConstructor(acquired => new TestCloseable(42, acquired))
      val resource = for {
        closeable <- ResourceOwner.forCloseable(newCloseable.apply _).acquire()
      } yield {
        withClue("after acquiring,") {
          newCloseable.hasBeenAcquired should be(true)
          closeable.value should be(42)
        }
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        withClue("after releasing,") {
          newCloseable.hasBeenAcquired should be(false)
        }
      }
    }
  }

  "a function returning a Future[AutoCloseable]" should {
    "convert to a ResourceOwner" in {
      val newCloseable =
        new MockConstructor(acquired => Future.successful(new TestCloseable(93, acquired)))
      val resource = for {
        closeable <- ResourceOwner.forFutureCloseable(newCloseable.apply _).acquire()
      } yield {
        withClue("after acquiring,") {
          newCloseable.hasBeenAcquired should be(true)
          closeable.value should be(93)
        }
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        withClue("after releasing,") {
          newCloseable.hasBeenAcquired should be(false)
        }
      }
    }
  }

  "a function returning an ExecutorService" should {
    "convert to a ResourceOwner" in {
      val testPromise = Promise[Unit]()
      val resource = for {
        executor <- ResourceOwner
          .forExecutorService(() => Executors.newFixedThreadPool(1))
          .acquire()
      } yield {
        executor.submit(() => testPromise.success(()))
        executor
      }

      for {
        _ <- resource.asFuture
        _ <- testPromise.future
        _ <- resource.release()
        executor <- resource.asFuture
      } yield {
        an[RejectedExecutionException] should be thrownBy executor.submit(() => 7)
      }
    }
  }

  "a function returning a Timer" should {
    "convert to a ResourceOwner" in {
      val testPromise = Promise[Unit]()
      val resource = for {
        timer <- ResourceOwner.forTimer(() => new Timer("test timer")).acquire()
      } yield {
        timer.schedule(new TimerTask {
          override def run(): Unit = testPromise.success(())
        }, 0)
        timer
      }

      for {
        _ <- resource.asFuture
        _ <- testPromise.future
        _ <- resource.release()
        timer <- resource.asFuture
      } yield {
        an[IllegalStateException] should be thrownBy timer.schedule(new TimerTask {
          override def run(): Unit = ()
        }, 0)
      }
    }
  }

  "a function returning an ActorSystem" should {
    "convert to a ResourceOwner" in {
      val testPromise = Promise[Int]()
      class TestActor extends Actor {
        @SuppressWarnings(Array("org.wartremover.warts.Any"))
        override def receive: Receive = {
          case value: Int => testPromise.success(value)
          case value => testPromise.failure(new IllegalArgumentException(s"$value"))
        }
      }

      val resource = for {
        actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem("TestActorSystem")).acquire()
        actor <- ResourceOwner
          .successful(actorSystem.actorOf(Props(new TestActor)))
          .acquire()
      } yield (actorSystem, actor)

      for {
        resourceFuture <- resource.asFuture
        (actorSystem, actor) = resourceFuture
        _ = actor ! 7
        result <- testPromise.future
        _ <- resource.release()
      } yield {
        result should be(7)
        an[IllegalStateException] should be thrownBy actorSystem.actorOf(Props(new TestActor))
      }
    }
  }

  "a function returning a Materializer" should {
    "convert to a ResourceOwner" in {
      val resource = for {
        actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem("TestActorSystem")).acquire()
        materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem)).acquire()
      } yield materializer

      for {
        materializer <- resource.asFuture
        numbers <- Source(1 to 10)
          .toMat(Sink.seq)(Keep.right[NotUsed, Future[Seq[Int]]])
          .run()(materializer)
        _ <- resource.release()
      } yield {
        numbers should be(1 to 10)
        an[IllegalStateException] should be thrownBy Source
          .single(0)
          .toMat(Sink.ignore)(Keep.right[NotUsed, Future[Done]])
          .run()(materializer)
      }
    }
  }

  "many resources in a sequence" should {
    "be able to be sequenced" in {
      val acquireOrder = mutable.Buffer[Int]()
      val releaseOrder = mutable.Buffer[Int]()
      val owners = (1 to 10).map(value =>
        new ResourceOwner[Int] {
          override def acquire()(implicit executionContext: ExecutionContext): Resource[Int] = {
            acquireOrder += value
            Resource(
              Future {
                value
              },
              v =>
                Future {
                  releaseOrder += v
              })
          }
      })
      val resources = owners.map(_.acquire())

      val resource = for {
        values <- Resource.sequence(resources)
      } yield {
        withClue("after sequencing,") {
          acquireOrder should be(1 to 10)
          values should be(1 to 10)
        }
        ()
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        withClue("after releasing,") {
          releaseOrder should be(10.to(1, step = -1))
        }
      }
    }
  }
}
