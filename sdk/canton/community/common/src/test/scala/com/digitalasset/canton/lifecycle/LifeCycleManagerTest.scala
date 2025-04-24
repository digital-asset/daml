// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.LifeCycleManager.ManagedResource
import com.digitalasset.canton.lifecycle.LifeCycleManagerTest.{
  LifeCycleManagerObservationResult,
  TestManagedResource,
  TestRunOnClosing,
}
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Try}

class LifeCycleManagerTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  "LifeCycleManager" when {
    "at the root" should {
      "manage ManagedResources" in {
        val lcm = LifeCycleManager.root("simple", 1.second, loggerFactory)

        val resources = (1 to 3).map(i => new TestManagedResource(s"resource$i"))
        val handles = resources.map(registerOrFail(lcm, _))

        handles(1).cancel()

        lcm.closeAsync().futureValue

        resources(0).releaseCount shouldBe 1
        resources(1).releaseCount shouldBe 0
        resources(2).releaseCount shouldBe 1
      }

      "release resources in priority order" in {
        val lcm = LifeCycleManager.root("priority", 1.second, loggerFactory)

        val releaseOrder = new AtomicReference[Seq[Int]](Seq.empty)

        val resources = (1 to 3).map(i =>
          new TestManagedResource(
            s"resource$i",
            _ => {
              releaseOrder.getAndUpdate(_ :+ i)
              Future.unit
            },
          )
        )

        resources.zipWithIndex.map { case (resource, index) =>
          registerOrFail(lcm, resource, priority = (-index).toShort)
        }

        lcm.closeAsync().futureValue

        releaseOrder.get() shouldBe Seq(3, 2, 1)
      }

      "reject registering resources after closing" in {
        val lcm = LifeCycleManager.root("too late", 1.second, loggerFactory)
        lcm.closeAsync().futureValue
        lcm.registerManaged(new TestManagedResource("after-closing")) shouldBe AbortedDueToShutdown
      }

      "register a resource multiple times" in {
        val lcm = LifeCycleManager.root("multiple times", 1.second, loggerFactory)
        val resource = new TestManagedResource("multi-registration resource")
        val handle1 = registerOrFail(lcm, resource)
        val handle2 = registerOrFail(lcm, resource)
        val handle3 = registerOrFail(lcm, resource)
        handle2.cancel() shouldBe true
        handle1.isScheduled shouldBe true
        handle3.isScheduled shouldBe true
        lcm.closeAsync().futureValue

        resource.releaseCount shouldBe 2
      }

      "release resources despite failure" in {
        val lcmName = "release failure"
        val lcm = LifeCycleManager.root(lcmName, 1.second, loggerFactory)
        val ex = new RuntimeException("Release failed")
        val resourceName = "failed resource"
        val resource1 = new TestManagedResource(resourceName, _ => Future.failed(ex))
        registerOrFail(lcm, resource1, priority = Short.MinValue)
        val resource2 = new TestManagedResource("good resource")
        registerOrFail(lcm, resource2, priority = Short.MaxValue)

        val errorF = loggerFactory.assertLogs(
          lcm.closeAsync(),
          _.warningMessage should include(
            s"Releasing managed resource '$resourceName' from manager '$lcmName' failed"
          ),
        )
        val error = errorF.failed.futureValue
        error shouldBe a[ShutdownFailedException]
        error.getMessage should include(s"Unable to close 'LifeCycleManager($lcmName)'")
        error.getSuppressed.toSeq shouldBe Seq(ex)
        resource2.releaseCount shouldBe 1
      }

      "report all release failures" in {
        val lcmName = "all release failure"
        val lcm = LifeCycleManager.root(lcmName, 1.second, loggerFactory)
        val ex1 = new RuntimeException("Release1 failed")
        val ex2 = new RuntimeException("Release2 failed")
        val resourceName1 = "async failed resource"
        val resource1 = new TestManagedResource(resourceName1, _ => Future.failed(ex1))
        registerOrFail(lcm, resource1)
        val resourceName2 = "sync failed resource"
        val resource2 = new TestManagedResource(resourceName2, _ => throw ex2)
        registerOrFail(lcm, resource2)

        val errorF = loggerFactory.assertLogs(
          lcm.closeAsync(),
          _.warningMessage should include(
            s"Releasing managed resource '$resourceName2' from manager '$lcmName' failed"
          ),
          _.warningMessage should include(
            s"Releasing managed resource '$resourceName1' from manager '$lcmName' failed"
          ),
        )
        val error = errorF.failed.futureValue
        error shouldBe a[ShutdownFailedException]
        error.getMessage should include(s"Unable to close 'LifeCycleManager($lcmName)'")
        error.getSuppressed.toSet shouldBe Set(ex1, ex2)
      }

      "release resources in parallel" in {
        val lcm = LifeCycleManager.root("parallel release", 1.second, loggerFactory)

        val count = 3
        val promises = (0 until count).map(_ => Promise[Unit]())

        def mkResource(index: Int): TestManagedResource = new TestManagedResource(
          s"resource$index",
          _ => {
            val rotated: Seq[Promise[Unit]] = (promises ++ promises).slice(index, index + count)
            rotated(0).success(())
            rotated.drop(1).parTraverse_(_.future).futureValue
            Future.unit
          },
        )

        val resources = (0 until count).map(mkResource)
        resources.foreach(lcm.registerManaged(_).failOnShutdown)

        lcm.closeAsync().futureValue
      }

      "closing is idempotent" in {
        val lcm = LifeCycleManager.root("idempotent", 1.second, loggerFactory)

        val promise = Promise[Unit]()
        val compF = lcm.synchronizeWithClosingF("promise")(promise.future).failOnShutdown

        val closeF1 = Future.unit.flatMap(_ => lcm.closeAsync())
        val closeF2 = lcm.closeAsync()

        closeF1.isCompleted shouldBe false
        closeF2.isCompleted shouldBe false

        promise.success(())

        compF.futureValue
        closeF1.futureValue
        closeF2.futureValue
      }

      "synchronize with computation before releasing" in {
        val lcm = LifeCycleManager.root("synchronizeWithClosing", 1.second, loggerFactory)
        val closeOrder = new AtomicReference[Seq[String]](Seq.empty)
        val resource = new TestManagedResource(
          "res",
          _ => {
            closeOrder.getAndUpdate(_ :+ "resource")
            Future.unit
          },
        )
        registerOrFail(lcm, resource)

        val closeF = lcm
          .synchronizeWithClosing("initiate closing from synchronization block") {
            val closeF = Future.unit.flatMap(_ => lcm.closeAsync())
            Threading.sleep(100)
            closeOrder.getAndUpdate(_ :+ "computation")
            closeF
          }
          .failOnShutdown

        closeF.futureValue
        closeOrder.get shouldBe Seq("computation", "resource")
      }

      "synchronize with asynchronous computation before releasing" in {
        val lcm = LifeCycleManager.root("synchronizeWithClosingF", 1.second, loggerFactory)
        val closeOrder = new AtomicReference[Seq[String]](Seq.empty)
        val resource = new TestManagedResource(
          "res",
          _ => {
            closeOrder.getAndUpdate(_ :+ "resource")
            Future.unit
          },
        )
        registerOrFail(lcm, resource)

        val promise = Promise[Unit]()
        val compF = lcm
          .synchronizeWithClosingF("initiate closing from asynchronous synchronization block") {
            promise.future.map { _ =>
              closeOrder.getAndUpdate(_ :+ "computation")
              ()
            }
          }
          .failOnShutdown

        val closeF = lcm.closeAsync()
        Threading.sleep(100)
        promise.success(())
        closeF.futureValue
        compF.futureValue
        closeOrder.get shouldBe Seq("computation", "resource")
      }

      "short-circuits synchronize after closing" in {
        val lcm = LifeCycleManager.root("synchronize too late", 1.second, loggerFactory)
        lcm.closeAsync().futureValue
        lcm.synchronizeWithClosing("short-circuit")(()) shouldBe AbortedDueToShutdown
      }

      "short-circuits synchronize if closing is in progress" in {
        val lcm = LifeCycleManager.root("synchronize concurrent", 1.second, loggerFactory)
        val promise = Promise[Unit]()
        val delayedCLosing =
          lcm.synchronizeWithClosingF("short-circuit")(promise.future).failOnShutdown
        val closeF = lcm.closeAsync()
        lcm.synchronizeWithClosing("short-circuit")(()) shouldBe AbortedDueToShutdown
        promise.success(())
        closeF.futureValue
        delayedCLosing.futureValue
      }

      "synchronize with failing computations" in {
        val lcm = LifeCycleManager.root("synchronizeWithClosing failure", 1.second, loggerFactory)

        val closeRef = new AtomicReference[Future[Unit]]()
        val ex = new RuntimeException("failing computation")

        val compT =
          Try(lcm.synchronizeWithClosing("initiate closing from failing synchronization block") {
            val closeF = lcm.closeAsync()
            closeRef.set(closeF)
            throw ex
          })

        compT shouldBe Failure(ex)
        closeRef.get.futureValue
      }

      "synchronize with asynchronous failing computations" in {
        val lcm = LifeCycleManager.root("synchronizeWithClosingF failure", 1.second, loggerFactory)

        val closeOrder = new AtomicReference[Seq[String]](Seq.empty)
        val resource = new TestManagedResource(
          "res",
          _ => {
            closeOrder.getAndUpdate(_ :+ "resource")
            Future.unit
          },
        )
        registerOrFail(lcm, resource)

        val ex = new RuntimeException("failing computation")
        val promise = Promise[Unit]()
        val compF1 = lcm
          .synchronizeWithClosingF(
            "initiate closing from failing asynchronous synchronization block"
          ) {
            promise.future.map { _ =>
              closeOrder.getAndUpdate(_ :+ "computation")
              throw ex
            }
          }
          .failOnShutdown

        val closeF = lcm.closeAsync()
        Threading.sleep(100)
        promise.success(())
        closeF.futureValue
        compF1.failed.futureValue shouldBe ex
        closeOrder.get shouldBe Seq("computation", "resource")
      }

      "synchronize with failing asynchronous computation" in {
        val lcm = LifeCycleManager.root(
          "synchronizeWithClosingF failure synchronous",
          1.second,
          loggerFactory,
        )

        val closeRef = new AtomicReference[Future[Unit]]()
        val ex = new RuntimeException("failing computation")

        val compT = Try(
          lcm.synchronizeWithClosingF(
            "initiate closing from failing asynchronous synchronization block"
          ) {
            val closeF = lcm.closeAsync()
            closeRef.set(closeF)
            (throw ex): Future[Unit]
          }
        )

        compT shouldBe Failure(ex)
        closeRef.get.futureValue
      }

      "time-box synchronization" in {
        val timeout = 100.milliseconds
        val lcm =
          LifeCycleManager.root("synchronizeWithClosingF non-termination", timeout, loggerFactory)

        Seq(1 to 10).foreach { i =>
          lcm.synchronizeWithClosingF(s"computation $i")(Future.never).failOnShutdown.discard
        }

        val closeF = loggerFactory.assertLogs(
          lcm.closeAsync(),
          _.warningMessage should include(
            s"Timeout $timeout expired, but tasks still running. Shutting down forcibly."
          ),
        )

        closeF.failed.futureValue shouldBe a[ShutdownFailedException]
      }

      "allow late completion of synchronization" in {
        val timeout = 100.milliseconds
        val lcm =
          LifeCycleManager.root("synchronizeWithClosingF slow", timeout, loggerFactory)

        val promise = Promise[Unit]()
        val compF = lcm.synchronizeWithClosingF("slow computation")(promise.future).failOnShutdown

        val resource = new TestManagedResource(
          "res",
          _ => {
            logger.debug("resource release called: slow computation finishes now")
            promise.success(())
            compF
          },
        )
        lcm.registerManaged(resource).failOnShutdown

        val closeF = loggerFactory.assertLogs(
          lcm.closeAsync(),
          _.warningMessage should include(
            s"Timeout $timeout expired, but readers are still active. Shutting down forcibly."
          ),
        )

        closeF.futureValue
      }

      "perform runOnClose before synchronization" in {
        val lcm = LifeCycleManager.root("runOnClose before synchronize", 1.second, loggerFactory)

        val promise = Promise[Unit]()

        val compF = lcm
          .synchronizeWithClosingF("computation completes via runOnClose")(promise.future)
          .failOnShutdown

        val runOnClose =
          new TestRunOnClosing("runOnClose before synchronize", () => promise.success(()))
        lcm.runOnClose(runOnClose).failOnShutdown

        val closeF = lcm.closeAsync()
        closeF.futureValue
        compF.futureValue
        runOnClose.runCount shouldBe 1
      }

      "log and swallow runOnClose errors" in {
        val lcm = LifeCycleManager.root("runOnClose before synchronize", 1.second, loggerFactory)

        val ex = new RuntimeException("runOnClose failure")
        val name = "runOnClose error"
        lcm.runOnClose(new TestRunOnClosing(name, () => throw ex)).failOnShutdown

        val closeF = loggerFactory.assertLogs(
          lcm.closeAsync(),
          entry => {
            entry.warningMessage should include(s"Task '$name' failed on closing!")
            entry.throwable should contain(ex)
          },
        )

        closeF.futureValue
      }

      "reject runOnClosing during closing" in {
        val lcm = LifeCycleManager.root("runOnClose too late", 1.second, loggerFactory)
        lcm.closeAsync().futureValue
        lcm.runOnClose(new TestRunOnClosing("tooLate")) shouldBe AbortedDueToShutdown
      }
    }

    "in a hierarchy" should {
      "be closed by the parent" in {
        val root = LifeCycleManager.root("root", 1.second, loggerFactory)
        val child1 = LifeCycleManager.dependent("child1", root, 1.second, loggerFactory)
        val child2 = LifeCycleManager.dependent("child2", root, 1.second, loggerFactory)

        val resource1 = new TestManagedResource("resource1")
        val resource2 = new TestManagedResource("resource2")

        child1.registerManaged(resource1).failOnShutdown
        child2.registerManaged(resource2).failOnShutdown

        root.closeAsync().futureValue
        resource1.releaseCount shouldBe 1
        resource2.releaseCount shouldBe 1
      }

      "respect priority order of children" in {
        val root = LifeCycleManager.root("root", 1.second, loggerFactory)
        val child1 = LifeCycleManager.dependent(
          "child1",
          root,
          1.second,
          loggerFactory,
          parentPriority = Short.MinValue,
        )
        val child2 = LifeCycleManager.dependent(
          "child2",
          root,
          1.second,
          loggerFactory,
          parentPriority = Short.MaxValue,
        )

        val closeOrder = new AtomicReference[Seq[Int]](Seq.empty)

        val resources = (0 until 3).map { i =>
          new TestManagedResource(
            s"resource$i",
            _ => {
              closeOrder.getAndUpdate(_ :+ i)
              Future.unit
            },
          )
        }
        root.registerManaged(resources(0)).failOnShutdown
        child2.registerManaged(resources(2)).failOnShutdown
        child1.registerManaged(resources(1)).failOnShutdown

        root.closeAsync().futureValue

        closeOrder.get() shouldBe Seq(1, 0, 2)
      }

      "propagate the close signal immediately" in {
        val root = LifeCycleManager.root("root", 1.second, loggerFactory)
        val child = LifeCycleManager.dependent("child", root, 1.second, loggerFactory)
        val grandchild = LifeCycleManager.dependent("grandchild", child, 1.second, loggerFactory)

        val closingStateObservations =
          new AtomicReference[Seq[LifeCycleManagerObservationResult[Boolean]]](Seq.empty)

        val runRoot = new TestRunOnClosing(
          "check child close status",
          () => {
            val childClosing = child.isClosing
            val grandchildClosing = grandchild.isClosing
            closingStateObservations.getAndUpdate(
              _ :+ LifeCycleManagerObservationResult(
                root,
                child,
                childClosing,
              ) :+ LifeCycleManagerObservationResult(
                root,
                grandchild,
                grandchildClosing,
              )
            )
          },
        )
        root.runOnClose(runRoot).failOnShutdown

        val runChild = new TestRunOnClosing(
          "check grandchild close status",
          () => {
            val grandChildClosing = grandchild.isClosing
            closingStateObservations.getAndUpdate(
              _ :+ LifeCycleManagerObservationResult(child, grandchild, grandChildClosing)
            )
          },
        )
        child.runOnClose(runChild).failOnShutdown

        root.closeAsync().futureValue

        closingStateObservations.get().toSet shouldBe Set(
          LifeCycleManagerObservationResult(root, child, true),
          LifeCycleManagerObservationResult(root, grandchild, true),
          LifeCycleManagerObservationResult(child, grandchild, true),
        )
      }

      "have the parent wait for children's closing" in {
        val root = LifeCycleManager.root("root", 1.second, loggerFactory)
        val child = LifeCycleManager.dependent("child", root, 2.second, loggerFactory)

        val promise = Promise[Unit]()
        val compF =
          child.synchronizeWithClosingF("child computation")(promise.future).failOnShutdown

        val closeF = root.closeAsync()
        always(durationOfSuccess = 500.milliseconds) {
          closeF.isCompleted shouldBe false
        }
        promise.success(())
        closeF.futureValue
        compF.futureValue
      }

      "close the child independently from the parent" in {
        val root = LifeCycleManager.root("root", 1.second, loggerFactory)
        val child = LifeCycleManager.dependent("child", root, 2.second, loggerFactory)

        val promise = Promise[Unit]()
        val compF =
          child.synchronizeWithClosingF("child computation")(promise.future).failOnShutdown

        val closeChildF = child.closeAsync()
        Threading.sleep(10)
        val closeRootF = root.closeAsync()
        // Make sure that the parent's close method does not complete before the child's closing has finished
        always(durationOfSuccess = 500.milliseconds) {
          closeRootF.isCompleted shouldBe false
        }
        promise.success(())
        closeChildF.futureValue
        closeRootF.futureValue
        compF.futureValue
      }

      "synchronize with closing happens independently across managers" in {
        val root = LifeCycleManager.root("root", 1.second, loggerFactory)
        val child = LifeCycleManager.dependent("child", root, 2.second, loggerFactory)

        val promise = Promise[Unit]()
        val compF =
          child.synchronizeWithClosingF("child computation")(promise.future).failOnShutdown

        val resource = new TestManagedResource(
          "unblock synchronize with closing of child",
          _ => {
            promise.success(())
            Future.unit
          },
        )
        root.registerManaged(resource)

        val closeF = root.closeAsync()
        compF.futureValue
        closeF.futureValue
      }

      "runOnClose happens everywhere before synchronization" in {
        val root = LifeCycleManager.root("root", 1.second, loggerFactory)
        val child = LifeCycleManager.dependent("child", root, 2.second, loggerFactory)

        val promise = Promise[Unit]()
        val rootF =
          root.synchronizeWithClosingF("root computation")(promise.future).failOnShutdown
        val childF =
          root.synchronizeWithClosingF("child computation")(promise.future).failOnShutdown

        val rootHandle = root.runOnClose(new TestRunOnClosing("at root")).failOnShutdown
        val childHandle = child.runOnClose(new TestRunOnClosing("at child")).failOnShutdown

        val closeF = root.closeAsync()
        eventually() {
          rootHandle.isScheduled shouldBe false
          childHandle.isScheduled shouldBe false
        }
        promise.success(())

        rootF.futureValue
        childF.futureValue
        closeF.futureValue
      }

      "close immediately if parent is closed" in {
        val root = LifeCycleManager.root("root", 1.second, loggerFactory)
        root.closeAsync().futureValue

        val child = LifeCycleManager.dependent("child", root, 2.second, loggerFactory)
        child.isClosing shouldBe true
        child.closeAsync().isCompleted shouldBe true
      }

      "close immediately if parent is closing concurrently" in {

        // While the child registers with the parent manager, the parent manager will try to clean up obsolete
        // runOnClose tasks. This test exploits this behavior to close the parent in the middle
        // of the registration process.

        def triggerCloseAfter(threshold: Int): Unit =
          withClue(s"attempting close after $threshold") {
            val root = LifeCycleManager.root("root", 1.second, loggerFactory)

            val doneInvocationCount = new AtomicInteger()

            val registrationMonitor = new RunOnClosing {
              override def name: String = "monitor"

              override def done: Boolean = {
                val invocationCount = doneInvocationCount.incrementAndGet()
                if (invocationCount >= threshold) { root.closeAsync().discard }
                false
              }

              override def run()(implicit traceContext: TraceContext): Unit = ()
            }
            root.runOnClose(registrationMonitor).failOnShutdown

            val child = LifeCycleManager.dependent("child", root, 2.second, loggerFactory)
            child.isClosing shouldBe true
            root.closeAsync().futureValue
          }

        // The child manager will register three tasks. So there is a separate
        // test case that closes after each of them.
        triggerCloseAfter(1)
        triggerCloseAfter(2)
        triggerCloseAfter(3)
      }
    }
  }

  private def registerOrFail(
      lcm: LifeCycleManager,
      resource: TestManagedResource,
      priority: Short = 0,
  ): LifeCycleRegistrationHandle =
    lcm
      .registerManaged(resource, priority)
      .failOnShutdown(s"Registering resource ${resource.name} failed due to shutdown")

}

object LifeCycleManagerTest {
  private[lifecycle] class TestManagedResource(
      override val name: String,
      onRelease: TraceContext => Future[Unit] = _ => Future.unit,
  ) extends ManagedResource {
    private val releaseCounter: AtomicInteger = new AtomicInteger()

    def releaseCount: Int = releaseCounter.get

    override protected def releaseByManager()(implicit traceContext: TraceContext): Future[Unit] = {
      releaseCounter.incrementAndGet().discard
      onRelease(traceContext)
    }
  }

  private[lifecycle] class TestRunOnClosing(
      override val name: String,
      onRun: () => Unit = () => (),
  ) extends RunOnClosing {
    private val runCounter: AtomicInteger = new AtomicInteger()

    def runCount: Int = runCounter.get

    override def done: Boolean = false
    override def run()(implicit traceContext: TraceContext): Unit = {
      runCounter.incrementAndGet().discard
      onRun()
    }
  }

  private final case class LifeCycleManagerObservationResult[+A](
      executingManager: LifeCycleManager,
      checkedManager: LifeCycleManager,
      result: A,
  )

}
