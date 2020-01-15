// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.{Files, Path}

import com.daml.ledger.on.filesystem.posix.DeleteFiles.deleteFiles
import org.scalatest.{AsyncWordSpec, BeforeAndAfterEach, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.global
import scala.concurrent.{ExecutionContext, Future}

class FileSystemLockSpec extends AsyncWordSpec with Matchers with BeforeAndAfterEach {
  private implicit val ec: ExecutionContext = global

  private var directory: Path = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    directory = Files.createTempDirectory(getClass.getSimpleName)
  }

  override def afterEach(): Unit = {
    if (directory != null) {
      deleteFiles(directory)
    }
    super.afterEach()
  }

  "a file system lock" should {
    "only allow one item to access it at a time" in {
      val operations = 10

      val lockPath = directory.resolve("lock")
      val counterPath = directory.resolve("counter")
      def readCounter(): Int = {
        Files.readAllLines(counterPath).get(0).toInt
      }
      def writeCounter(counter: Int): Unit = {
        Files.write(counterPath, Seq(counter.toString).asJava)
        ()
      }
      writeCounter(0)

      for {
        output <- Future.sequence((1 to operations).map { _ =>
          val lock = new FileSystemLock(lockPath)
          lock.run {
            var counter = readCounter()
            counter += 1
            writeCounter(counter)
            counter
          }
        })
      } yield {
        readCounter() should be(operations)
        output should have size operations.toLong
        output should contain theSameElementsAs (1 to operations)
      }
    }
  }
}
