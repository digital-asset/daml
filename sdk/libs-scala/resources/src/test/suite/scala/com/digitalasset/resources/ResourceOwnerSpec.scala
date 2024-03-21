// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.{
  CopyOnWriteArrayList,
  CopyOnWriteArraySet,
  Executors,
  RejectedExecutionException,
}
import java.util.{Timer, TimerTask}
import com.daml.resources.FailingResourceOwner.{
  FailingResourceFailedToOpen,
  TriedToReleaseAFailedResource,
}
import com.daml.resources.{Resource => AbstractResource}
import com.daml.timer.Delayed
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.annotation.nowarn
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

final class ResourceOwnerSpec extends AsyncWordSpec with Matchers {
  private type Resource[+T] = AbstractResource[TestContext, T]
  private val Resource = new ResourceFactories[TestContext]
  private val Factories = new ResourceOwnerFactories[TestContext] {
    override protected implicit val hasExecutionContext: HasExecutionContext[TestContext] =
      TestContext.`TestContext has ExecutionContext`
  }

  private implicit val context: TestContext = new TestContext(ExecutionContext.global)

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

    "treat single releases idempotently, only releasing once regardless of the number of calls" in {
      val owner = TestResourceOwner(7)
      val resource = owner.acquire()

      for {
        _ <- resource.asFuture
        _ <- resource.release()
        // if `TestResourceOwner`'s release function is called twice, it'll fail
        _ <- resource.release()
      } yield {
        owner.hasBeenAcquired should be(false)
      }
    }

    "treat nested releases idempotently, only releasing once regardless of the number of calls" in {
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

      @nowarn(
        "msg=parameter value resourceA .* is never used"
      ) // stray reference inserted by withFilter
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

    "transform success into another success" in {
      val owner = TestResourceOwner(5)
      val resource = owner.acquire()

      val transformedResource = resource.transform {
        case Success(value) =>
          Success(value + 1)
        case Failure(_) =>
          fail("The failure path should never be called.")
      }

      for {
        value <- transformedResource.asFuture
        _ <- transformedResource.release()
      } yield {
        withClue("after releasing,") {
          value should be(6)
        }
      }
    }

    "transform success into a failure" in {
      val owner = TestResourceOwner(9)
      val resource = owner.acquire()

      val transformedResource = resource.transform[Int] {
        case Success(value) =>
          Failure(new RuntimeException(s"Oh no! The value was $value."))
        case Failure(_) =>
          fail("The failure path should never be called.")
      }

      for {
        exception <- transformedResource.asFuture.failed
        _ <- transformedResource.release()
      } yield {
        withClue("after releasing,") {
          exception.getMessage should be("Oh no! The value was 9.")
        }
      }
    }

    "transform failure into a success" in {
      val owner = new TestResourceOwner[Int](
        Future.failed(new Exception("This one didn't work.")),
        _ => Future.failed(new TriedToReleaseAFailedResource),
      )
      val resource = owner.acquire()

      val transformedResource = resource.transform {
        case Success(_) =>
          fail("The success path should never be called.")
        case Failure(exception) =>
          Success(exception.getMessage)
      }

      for {
        value <- transformedResource.asFuture
        _ <- transformedResource.release()
      } yield {
        withClue("after releasing,") {
          value should be("This one didn't work.")
        }
      }
    }

    "transform failure into another failure" in {
      val owner = new TestResourceOwner[Int](
        Future.failed(new Exception("This also didn't work.")),
        _ => Future.failed(new TriedToReleaseAFailedResource),
      )
      val resource = owner.acquire()

      val transformedResource = resource.transform[Int] {
        case Success(_) =>
          fail("The success path should never be called.")
        case Failure(exception) =>
          Failure(new Exception(exception.getMessage + " Boo."))
      }

      for {
        exception <- transformedResource.asFuture.failed
        _ <- transformedResource.release()
      } yield {
        withClue("after releasing,") {
          exception.getMessage should be("This also didn't work. Boo.")
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
        case Success(_) =>
          Resource.failed[String](new IllegalStateException("Unexpected success."))
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
  "using a resource" should {
    "perform the given behavior" in {
      val owner = Factories.successful(42)
      owner.use { value =>
        value should be(42)
      }
    }

    "clean up afterwards" in {
      val owner = TestResourceOwner(42)
      owner
        .use { value =>
          owner.hasBeenAcquired should be(true)
          value should be(42)
        }
        .map { _ =>
          owner.hasBeenAcquired should be(false)
        }
    }

    "report errors in acquisition, even after usage" in {
      val owner = FailingResourceOwner[Int]()
      owner
        .use { _ =>
          fail("Can't use a failed resource.")
        }
        .failed
        .map { exception =>
          exception should be(a[FailingResourceFailedToOpen])
        }
    }

    "report errors in usage" in {
      val owner = TestResourceOwner(54)
      owner
        .use { _ =>
          owner.hasBeenAcquired should be(true)
          sys.error("Uh oh.")
        }
        .failed
        .map { exception =>
          owner.hasBeenAcquired should be(false)
          exception.getMessage should be("Uh oh.")
        }
    }
  }

  "a pure value" should {
    "convert to a ResourceOwner" in {
      val resource = for {
        value <- Factories.successful("Hello!").acquire()
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

      val resource = Factories.failed(ExampleThrowable).acquire()

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
        value <- Factories.forTry(() => Success(49)).acquire()
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
        value <- Factories.forFuture(() => Future.successful(54)).acquire()
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
        value <- Factories
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
        closeable <- Factories.forCloseable(newCloseable.apply _).acquire()
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
        closeable <- Factories.forFutureCloseable(newCloseable.apply _).acquire()
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

  "a function returning a class with a release method" should {
    "convert to a ResourceOwner" in {
      val newReleasable = new MockConstructor(acquired =>
        // A releasable is just a more generic closeable
        new TestCloseable(93, acquired)
      )

      val resource = for {
        releasable <- Factories
          .forReleasable(newReleasable.apply _)(r => Future(r.close()))
          .acquire()
      } yield {
        withClue("after acquiring,") {
          newReleasable.hasBeenAcquired should be(true)
          releasable.value should be(93)
        }
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        withClue("after releasing,") {
          newReleasable.hasBeenAcquired should be(false)
        }
      }
    }
  }

  "a function returning an ExecutorService" should {
    "convert to a ResourceOwner" in {
      val testPromise = Promise[Unit]()
      val resource = for {
        executor <- Factories
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

    "cause an exception if the result is the execution context, to avoid deadlock upon release" in {
      implicit val executionContext: ExecutionContextExecutorService =
        ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
      implicit val context: TestContext = new TestContext(executionContext)

      val resource = Factories.forExecutorService(() => executionContext).acquire()

      for {
        throwable <- resource.asFuture.failed
        _ <- resource.release()
      } yield {
        throwable should be(a[ExecutorServiceResourceOwner.CannotAcquireExecutionContext])
      }
    }

    "cause an exception if the result is wrapping the execution context, to avoid deadlock upon release" in {
      val executorService = Executors.newCachedThreadPool()
      implicit val executionContext: ExecutionContext =
        ExecutionContext.fromExecutorService(executorService)
      implicit val context: TestContext = new TestContext(executionContext)

      val resource = Factories.forExecutorService(() => executorService).acquire()

      for {
        throwable <- resource.asFuture.failed
        _ <- resource.release()
      } yield {
        throwable should be(a[ExecutorServiceResourceOwner.CannotAcquireExecutionContext])
      }
    }
  }

  "a function returning a Timer" should {
    "convert to a ResourceOwner" in {
      val testPromise = Promise[Unit]()
      val resource = for {
        timer <- Factories.forTimer(() => new Timer("test timer")).acquire()
      } yield {
        timer.schedule(
          new TimerTask {
            override def run(): Unit = testPromise.success(())
          },
          0,
        )
        timer
      }

      for {
        _ <- resource.asFuture
        _ <- testPromise.future
        _ <- resource.release()
        timer <- resource.asFuture
      } yield {
        an[IllegalStateException] should be thrownBy timer.schedule(
          new TimerTask {
            override def run(): Unit = ()
          },
          0,
        )
      }
    }

    "convert to a ResourceOwner, which ensures, that no TimerTask is running after the resource is released" in {
      val finishLongTakingTask = Promise[Unit]()
      val timerResource = Factories.forTimer(() => new Timer("test timer")).acquire()
      for {
        timer <- timerResource.asFuture
        (timerReleased, extraTimerTaskStarted) = {
          timer.schedule(
            new TimerTask {
              override def run(): Unit = {
                Await.result(finishLongTakingTask.future, Duration(10, "seconds"))
              }
            },
            0L,
          )
          info("As scheduled a long taking task")
          val released = timerResource.release()
          info("And as triggered release of the timer resource")
          Thread.sleep(100)
          info("And as waiting 100 millis")
          released.isCompleted shouldBe false
          info("The release should not be completed yet")
          val extraTimerTaskStarted = Promise[Unit]()
          timer.schedule(
            new TimerTask {
              override def run(): Unit = extraTimerTaskStarted.success(())
            },
            0L,
          )
          info("And scheduling a further task is still possible")
          finishLongTakingTask.success(())
          info("As completing the currently running timer task")
          (released, extraTimerTaskStarted.future)
        }
        _ <- timerReleased
      } yield {
        info("Timer released")
        an[IllegalStateException] should be thrownBy timer.schedule(
          new TimerTask {
            override def run(): Unit = ()
          },
          0,
        )
        info("And scheduling new task on the released timer, is not possible anymore")
        Thread.sleep(100)
        extraTimerTaskStarted.isCompleted shouldBe false
        info("And after waiting 100 millis, the additional task is still not started")
        succeed
      }
    }
  }

  "many resources in a sequence" should {
    "be able to be sequenced" in {
      val acquireOrder = new CopyOnWriteArrayList[Int]().asScala
      val released = new CopyOnWriteArraySet[Int]().asScala
      val owners = (1 to 10).map(value =>
        new AbstractResourceOwner[TestContext, Int] {
          override def acquire()(implicit context: TestContext): Resource[Int] = {
            acquireOrder += value
            Resource(Future(value))(v =>
              Future {
                released += v
                ()
              }
            )
          }
        }
      )
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
          released.toSet should be((1 to 10).toSet)
        }
      }
    }

    "sequence, ignoring values if asked" in {
      val acquired = new CopyOnWriteArraySet[Int]().asScala
      val released = new CopyOnWriteArraySet[Int]().asScala
      val owners = (1 to 10).map(value =>
        new AbstractResourceOwner[TestContext, Int] {
          override def acquire()(implicit context: TestContext): Resource[Int] =
            Resource(Future {
              acquired += value
              value
            })(v =>
              Future {
                released += v
                ()
              }
            )
        }
      )
      val resources = owners.map(_.acquire())

      val resource = for {
        values <- Resource.sequenceIgnoringValues(resources)
      } yield {
        withClue("after sequencing,") {
          acquired.toSet should be((1 to 10).toSet)
          values should be(())
        }
        ()
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        withClue("after releasing,") {
          released.toSet should be((1 to 10).toSet)
        }
      }
    }

    "acquire and release in parallel" in {
      val acquireOrder = new CopyOnWriteArrayList[Int]().asScala
      val releaseOrder = new CopyOnWriteArrayList[Int]().asScala
      val owners = (4 to 1 by -1).map(value =>
        new AbstractResourceOwner[TestContext, Int] {
          override def acquire()(implicit context: TestContext): Resource[Int] = {
            Resource(Delayed.by((value * 200).milliseconds) {
              acquireOrder += value
              value
            }) { v =>
              Delayed.by((v * 200).milliseconds) {
                releaseOrder += v
                ()
              }
            }
          }
        }
      )
      val resources = owners.map(_.acquire())

      val resource = for {
        values <- Resource.sequence(resources)
      } yield {
        withClue("during acquisition,") {
          acquireOrder should be(1 to 4)
        }
        withClue("after sequencing,") {
          values should be(4 to 1 by -1)
        }
        ()
      }

      for {
        _ <- resource.asFuture
        _ <- resource.release()
      } yield {
        withClue("after releasing,") {
          releaseOrder should be(1 to 4)
        }
      }
    }
  }
}
