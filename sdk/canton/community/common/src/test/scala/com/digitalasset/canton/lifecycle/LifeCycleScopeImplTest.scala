// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.lifecycle.LifeCycleManagerTest.{
  TestManagedResource,
  TestRunOnClosing,
}
import com.digitalasset.canton.lifecycle.LifeCycleScopeImpl.ThereafterTryUnlessShutdownFContent
import com.digitalasset.canton.lifecycle.LifeCycleScopeImplTest.TryUnlessShutdownFFixture
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureThereafterTest, ThereafterTest, TryThereafterTest}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class LifeCycleScopeImplTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ThereafterTest {

  "The empty scope" should {
    "never close" in {
      LifeCycleScopeImpl.empty.isClosing shouldBe false
    }

    "always accept any task" in {
      val scope = LifeCycleScopeImpl.empty
      val handle = scope.runOnClose(new TestRunOnClosing("empty scope task")).failOnShutdown
      handle.cancel() shouldBe true
    }

    "always run synchronize-with-closing tasks" in {
      val scope = LifeCycleScopeImpl.empty
      val hasRun = new AtomicBoolean()
      scope.synchronizeWithClosing("empty synchronize")(hasRun.set(true)).failOnShutdown
      hasRun.get() shouldBe true
    }

    "always run asynchronous synchronize-with-closing tasks" in {
      val scope = LifeCycleScopeImpl.empty
      val hasRun = new AtomicBoolean()
      scope
        .synchronizeWithClosingF("empty synchronize")(Future(hasRun.set(true)))
        .failOnShutdown
        .futureValue
      hasRun.get() shouldBe true
    }
  }

  "A scope with a single manager" should {

    "refuse tasks and synchronization when closing is in progress" in {
      val manager = LifeCycleManager.root("single manager", 1.second, loggerFactory)
      val promise = Promise[Unit]()
      val compF = manager.synchronizeWithClosingF("delay closing")(promise.future).failOnShutdown

      val scope = new LifeCycleScopeImpl(Set(manager))
      val closeF = manager.closeAsync()

      scope.runOnClose(new TestRunOnClosing("task during closing")) shouldBe AbortedDueToShutdown
      scope.synchronizeWithClosing("synchronize during closing") {
        fail("This should not run")
      } shouldBe AbortedDueToShutdown
      scope.synchronizeWithClosingF("async synchronize during closing") {
        fail("This should not run"): Unit
        Future.unit
      } shouldBe AbortedDueToShutdown

      promise.success(())
      compF.futureValue
      closeF.futureValue

      scope.runOnClose(new TestRunOnClosing("task after closing")) shouldBe AbortedDueToShutdown
      scope.synchronizeWithClosing("synchronize after closing") {
        fail("This should not run")
      } shouldBe AbortedDueToShutdown
      scope.synchronizeWithClosingF("async synchronize after closing") {
        fail("This should not run"): Unit
        Future.unit
      } shouldBe AbortedDueToShutdown
    }

    "support cancellation of tasks that have not yet run during closing" in {
      // This test relies on RunOnClosing tasks being executed sequentially.
      // If we change this so that they can execute in parallel, we will need a new strategy for testing this behavior.

      val manager = LifeCycleManager.root("single manager", 1.second, loggerFactory)
      val scope = new LifeCycleScopeImpl(Set(manager))
      val handle2Ref = new AtomicReference[LifeCycleRegistrationHandle]()

      val handle1Cancelled = new AtomicBoolean()
      val handle2Cancelled = new AtomicBoolean()

      val task1 = new TestRunOnClosing(
        "task1 cancellation during closing",
        () => handle2Cancelled.set(handle2Ref.get().cancel()),
      )
      val handle1 = scope.runOnClose(task1).failOnShutdown

      val task2 = new TestRunOnClosing(
        "task2 cancellation during closing",
        () => handle1Cancelled.set(handle1.cancel()),
      )
      val handle2 = scope.runOnClose(task2).failOnShutdown
      handle2Ref.set(handle2)

      manager.closeAsync().futureValue

      // As the tasks don't execute concurrently, the one that went first should have cancelled the other.
      handle1Cancelled.get() should not be handle2Cancelled.get()

      task1.runCount shouldBe (if (handle1Cancelled.get()) 0 else 1)
      task2.runCount shouldBe (if (handle2Cancelled.get()) 0 else 1)
    }
  }

  def mkTestResource(order: AtomicReference[Seq[String]], index: Int): TestManagedResource =
    new TestManagedResource(
      s"test resource$index",
      _ => {
        order.getAndUpdate(_ :+ "test resource")
        Future.unit
      },
    )

  "A scope with multiple managers" should {
    "close as soon as the first manager starts closing" in {
      val manager1 = LifeCycleManager.root("manager1", 1.second, loggerFactory)
      val manager2 = LifeCycleManager.root("manager2", 1.second, loggerFactory)
      val promise = Promise[Unit]()
      val compF = manager1.synchronizeWithClosingF("delay closing")(promise.future).failOnShutdown

      val scope = new LifeCycleScopeImpl(Set(manager1, manager2))
      scope.isClosing shouldBe false

      val closeF = manager1.closeAsync()
      scope.isClosing shouldBe true

      promise.success(())
      compF.futureValue
      closeF.futureValue
      scope.isClosing shouldBe true
    }

    "run tasks while the first manager is closing" in {
      val manager1 = LifeCycleManager.root("manager1", 1.second, loggerFactory)
      val manager2 = LifeCycleManager.root("manager2", 1.second, loggerFactory)
      val promise = Promise[Unit]()
      val compF = manager1.synchronizeWithClosingF("delay closing")(promise.future).failOnShutdown

      val scope = new LifeCycleScopeImpl(Set(manager1, manager2))
      val task = new TestRunOnClosing("scope task")
      val handle = scope.runOnClose(task).failOnShutdown

      handle.isScheduled shouldBe true

      val closeF = manager1.closeAsync()
      eventually() {
        task.runCount shouldBe 1
      }
      handle.isScheduled shouldBe false
      handle.cancel() shouldBe false

      promise.success(())
      compF.futureValue
      closeF.futureValue
    }

    "synchronize-with-closing tasks synchronize with all managers" in {
      val manager1 = LifeCycleManager.root("manager1", 1.second, loggerFactory)
      val manager2 = LifeCycleManager.root("manager2", 1.second, loggerFactory)
      val scope = new LifeCycleScopeImpl(Set(manager1, manager2))

      val order = new AtomicReference[Seq[String]](Seq.empty)
      manager1.registerManaged(mkTestResource(order, 1)).failOnShutdown
      manager2.registerManaged(mkTestResource(order, 2)).failOnShutdown

      val closeRef1 = new AtomicReference[Future[Unit]]()
      val closeRef2 = new AtomicReference[Future[Unit]]()
      scope.synchronizeWithClosing("close manager from within") {
        order.getAndUpdate(_ :+ "start")
        closeRef1.set(manager1.closeAsync())
        logger.debug("Give the first manager a bit of time to progress on its closing")
        Threading.sleep(10)
        closeRef2.set(manager2.closeAsync())
        logger.debug("Give the second manager a bit of time to progress on its closing")
        Threading.sleep(10)
        order.getAndUpdate(_ :+ "end")
      }

      closeRef1.get.futureValue
      closeRef2.get.futureValue
      order.get shouldBe Seq("start", "end", "test resource", "test resource")
    }

    "asynchronous synchronize-with-closing tasks synchronize with all managers" in {
      val manager1 = LifeCycleManager.root("manager1", 1.second, loggerFactory)
      val manager2 = LifeCycleManager.root("manager2", 1.second, loggerFactory)
      val scope = new LifeCycleScopeImpl(Set(manager1, manager2))

      val order = new AtomicReference[Seq[String]](Seq.empty)
      manager1.registerManaged(mkTestResource(order, 1)).failOnShutdown
      manager2.registerManaged(mkTestResource(order, 2)).failOnShutdown

      val promise = Promise[Unit]()

      val compF = scope
        .synchronizeWithClosingF("async computation") {
          order.getAndUpdate(_ :+ "start")
          promise.future.map { _ =>
            order.getAndUpdate(_ :+ "end")
            ()
          }
        }
        .failOnShutdown

      val closeF1 = manager1.closeAsync()
      always(durationOfSuccess = 100.milliseconds) {
        closeF1.isCompleted shouldBe false
      }
      val closeF2 = manager2.closeAsync()
      always(durationOfSuccess = 100.milliseconds) {
        closeF2.isCompleted shouldBe false
      }
      promise.success(())
      compF.futureValue
      closeF1.futureValue
      closeF2.futureValue

      order.get shouldBe Seq("start", "end", "test resource", "test resource")
    }

    "log exceptions thrown in run-on-close tasks exactly once" in {
      val manager1 = LifeCycleManager.root("manager1", 1.second, loggerFactory)
      val manager2 = LifeCycleManager.root("manager2", 1.second, loggerFactory)
      val scope = new LifeCycleScopeImpl(Set(manager1, manager2))

      val ex = new RuntimeException("Task failure")
      val taskName = "failing task"
      val closeRef2 = new AtomicReference[Future[Unit]]()
      scope
        .runOnClose(
          new TestRunOnClosing(
            taskName,
            () => {
              closeRef2.set(manager2.closeAsync())
              Threading.sleep(10)
              throw ex
            },
          )
        )
        .failOnShutdown

      loggerFactory.assertLogs(
        manager1.closeAsync().futureValue,
        // Only one log entry even though probably both managers invoke the registered task.
        logEntry => {
          logEntry.warningMessage should include(s"Task '$taskName' failed on closing!")
          logEntry.throwable should contain(ex)
        },
      )
      closeRef2.get.futureValue
    }

    "delay closing of all managers until RunOnClosing tasks have finished" in {
      val manager1 = LifeCycleManager.root("manager1", 1.second, loggerFactory)
      val manager2 = LifeCycleManager.root("manager2", 1.second, loggerFactory)
      val scope = new LifeCycleScopeImpl(Set(manager1, manager2))

      val order = new AtomicReference[Seq[String]](Seq.empty)
      manager1.registerManaged(mkTestResource(order, 1)).failOnShutdown
      manager2.registerManaged(mkTestResource(order, 2)).failOnShutdown

      val closeRef2 = new AtomicReference[Future[Unit]]()

      val task = new RunOnClosing {
        override def name: String = "sync with multiple managers"

        private val invoked = new AtomicBoolean()

        override def done: Boolean = invoked.get()

        override def run()(implicit traceContext: TraceContext): Unit = {
          logger.debug("Mark this task as done")
          invoked.set(true)
          order.getAndUpdate(_ :+ "start")
          logger.debug("Poke the second manager to remove obsolete tasks")
          manager2
            .runOnClose(new TestRunOnClosing("trigger removal of obsolete tasks"))
            .failOnShutdown
          // If `closeAsync` executed the RunOnClosing tasks synchronously,
          // the lazy val memoization would fail due to re-entrancy and we'd execute this run method again
          closeRef2.set(manager2.closeAsync())
          logger.debug("Give the second manager a bit of time to progress on its closing")
          Threading.sleep(10)
          order.getAndUpdate(_ :+ "end")
        }
      }
      scope.runOnClose(task).failOnShutdown

      manager1.closeAsync().futureValue
      closeRef2.get.futureValue

      order.get shouldBe Seq("start", "end", "test resource", "test resource")
    }
  }

  "ThereafterTryUnlessShutdownF" when {
    "used with Try" should {
      behave like thereafter(
        LifeCycleScopeImpl.ThereafterTryUnlessShutdownF.instance[Try],
        new TryUnlessShutdownFFixture(TryThereafterTest.fixture),
      )
    }
    "used with Future" should {
      behave like thereafter(
        LifeCycleScopeImpl.ThereafterTryUnlessShutdownF.instance[Future],
        new TryUnlessShutdownFFixture(FutureThereafterTest.fixture),
      )
    }
  }
}

object LifeCycleScopeImplTest {
  private class TryUnlessShutdownFFixture[F[_], Content[_]](
      val base: ThereafterTest.Fixture[F, Content]
  ) extends ThereafterTest.Fixture[
        Lambda[a => Try[UnlessShutdown[F[a]]]],
        ThereafterTryUnlessShutdownFContent[Content, *],
      ] {
    override type X = base.X
    type FF[A] = Try[UnlessShutdown[F[A]]]
    override def fromTry[A](x: Try[A]): FF[A] = Success(Outcome(base.fromTry(x)))
    override def fromContent[A](content: Try[UnlessShutdown[Content[A]]]): FF[A] =
      content.map(_.map(base.fromContent))
    override def isCompleted[A](x: FF[A]): Boolean = x match {
      case Success(Outcome(fa)) => base.isCompleted(fa)
      case _ => true
    }
    override def await[A](x: FF[A]): ThereafterTryUnlessShutdownFContent[Content, A] =
      x.map(_.map(base.await))

    override def contents: Seq[ThereafterTryUnlessShutdownFContent[Content, X]] =
      base.contents.map(c => Success(Outcome(c))) ++
        Seq(Success(AbortedDueToShutdown), Failure(new RuntimeException("test")))

    @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
    override def theContent[A](content: ThereafterTryUnlessShutdownFContent[Content, A]): A =
      base.theContent(
        content.get.onShutdown(throw new NoSuchElementException(("AbortedDueToShutdown")))
      )
  }
}
