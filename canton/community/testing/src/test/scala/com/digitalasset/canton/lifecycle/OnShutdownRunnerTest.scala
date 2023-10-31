// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.*

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

object OnShutdownRunnerTest {
  private class TestResource() extends AutoCloseable with OnShutdownRunner with NamedLogging {
    override protected def onFirstClose(): Unit = ()
    override val loggerFactory = NamedLoggerFactory.root
    override def close(): Unit = super.close()
  }
}

class OnShutdownRunnerTest extends AnyWordSpec with Matchers with NoTracing with Eventually {
  import OnShutdownRunnerTest.*

  "OnShutdownRunner" should {
    "run all shutdown tasks" in {

      var shutdownTasks: Seq[String] = Seq.empty

      val closeable = new TestResource()
      closeable.runOnShutdown_(new RunOnShutdown {
        override val name = "first"
        override val done = false

        override def run() = {
          shutdownTasks = shutdownTasks :+ "first"
        }
      })
      closeable.runOnShutdown_(new RunOnShutdown {
        override val name = "second"
        override val done = false

        override def run() = {
          shutdownTasks = shutdownTasks :+ "second"
        }
      })
      closeable.close()

      shutdownTasks.toSet shouldBe Set("first", "second")
    }

    "behave correctly if races occur during shutdown" in {
      val shutdownTasks = new ConcurrentHashMap[Int, Unit]()
      val closeable = new TestResource()
      val total = 100

      // Start by adding some shutdown tasks
      (0 to total / 2).foreach { i =>
        closeable.runOnShutdown_(new ConcurrentRunOnShutdownHelperClass(shutdownTasks, i))
      }

      // Then add another chunk each in it's own thread, and after a few close the closeable
      val threads = (total / 2 + 1 to total).map { i =>
        val t = new Thread(() => {
          closeable.runOnShutdown_(new ConcurrentRunOnShutdownHelperClass(shutdownTasks, i))
        })
        t.start()

        // Halfway through close the closeable
        if (i == (total * 0.75).toInt) closeable.close()

        t
      }

      eventually {
        // We should run all the tasks once and only once
        shutdownTasks.keySet().asScala should contain theSameElementsAs (0 to total)
      }
      // Make sure all threads complete
      threads.foreach(_.join())
    }

    "allow to cancel shutdown tasks" in {
      var shutdownTasks: Seq[String] = Seq.empty

      val closeable = new TestResource()
      closeable.runOnShutdown_(new RunOnShutdown {
        override val name = "first"
        override val done = false

        override def run() = {
          shutdownTasks = shutdownTasks :+ "first"
        }
      })
      val token = closeable.runOnShutdown(new RunOnShutdown {
        override val name = "second"
        override val done = false

        override def run() = {
          shutdownTasks = shutdownTasks :+ "second"
        }
      })
      closeable.runOnShutdown_(new RunOnShutdown {
        override val name = "third"
        override val done = false

        override def run() = {
          shutdownTasks = shutdownTasks :+ "third"
        }
      })
      closeable.cancelShutdownTask(token)

      closeable.close()

      shutdownTasks.toSet shouldBe Set("first", "third")
    }
  }

  private class ConcurrentRunOnShutdownHelperClass(
      shutdownTasks: ConcurrentHashMap[Int, Unit],
      i: Int,
  ) extends RunOnShutdown {
    override val name = i.toString
    override val done = shutdownTasks.contains(i)
    override def run() = {
      shutdownTasks.put(i, ())
    }
  }
}
