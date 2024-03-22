// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import better.files.File.RandomAccessMode
import com.digitalasset.canton.UniquePortGenerator.retryLock
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressingLogger}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.channels.FileLock
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future, blocking}

class UniquePortGeneratorTest
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with HasExecutionContext {
  behavior of "UniquePortGenerator"

  override protected def loggerFactory: NamedLoggerFactory = SuppressingLogger(getClass)

  var lock: Option[FileLock] = None

  it should "not throw on double lock request" in {
    UniquePortGenerator.SharedPortNumFile.usingLock(RandomAccessMode.readWrite) { f =>
      lock = Some(retryLock(100, 100) {
        blocking(synchronized(f.lock()))
      })

      val lockReleaseFuture = Future {
        Threading.sleep(1000)
        lock.foreach(_.release())
      }

      noException shouldBe thrownBy {
        UniquePortGenerator.next
      }

      Await.result(lockReleaseFuture, 3 seconds)
    }
  }

  override def afterAll(): Unit = {
    lock.foreach { l =>
      if (l.isValid) {
        l.release()
      }
    }
  }

}
