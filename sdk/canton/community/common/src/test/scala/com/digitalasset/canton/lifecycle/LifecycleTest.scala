// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.Lifecycle.close
import com.digitalasset.canton.logging.LogEntry
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable

class LifecycleTest extends AnyWordSpec with BaseTest {

  case class FailingAutoCloseable(t: Throwable, description: String) extends AutoCloseable {
    override def close(): Unit = throw t
    override def toString: String = description
  }

  "close" should {
    "return happily if everything closes fine" in {
      close(() => ())(logger)
    }
    "close all items in order" in {
      val closed = mutable.Buffer[Int]()

      close(
        () => closed.append(1),
        () => closed.append(2),
        () => closed.append(3),
      )(logger)

      closed shouldBe Seq(1, 2, 3)
    }
    "throw a shutdown exception if something throws" in {
      val underlyingException = new RuntimeException("😱")

      val thrown = loggerFactory.assertLogs(
        the[ShutdownFailedException] thrownBy close(
          FailingAutoCloseable(underlyingException, "kebab")
        )(logger),
        entry => {
          entry.warningMessage shouldBe "Closing 'kebab' failed! Reason:"
          entry.throwable shouldBe Some(underlyingException)
        },
      )
      thrown.getMessage shouldBe "Unable to close 'kebab'."
    }
    "when multiple items throw an exception" should {
      val closeables =
        (1 to 3).map(i => FailingAutoCloseable(new RuntimeException(s"error-$i"), s"component-$i"))

      "throw and log" in {
        val thrown = loggerFactory.assertLogs(
          the[ShutdownFailedException] thrownBy close(closeables: _*)(logger),
          closeables.map[LogEntry => Assertion](closeable =>
            entry => {
              entry.warningMessage shouldBe s"Closing '${closeable.description}' failed! Reason:"
              entry.throwable shouldBe Some(closeable.t)
            }
          ): _*
        )
        thrown.getMessage shouldBe "Unable to close Seq('component-1', 'component-2', 'component-3')."
      }
    }
  }
}
