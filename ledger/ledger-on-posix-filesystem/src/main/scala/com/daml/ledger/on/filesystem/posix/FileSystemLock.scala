// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.{FileSystemException, Files, Path, StandardCopyOption}

import com.daml.ledger.on.filesystem.posix.DeleteFiles.deleteFiles
import com.daml.ledger.on.filesystem.posix.FileSystemLock._
import com.digitalasset.timer.RetryStrategy

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class FileSystemLock(location: Path) {
  private val random: Random = new Random()

  def run[T](body: => T)(implicit executionContext: ExecutionContext): Future[T] =
    for {
      _ <- acquire()
      result = body
    } yield {
      release()
      result
    }

  private def acquire()(implicit executionContext: ExecutionContext): Future[Unit] = {
    val randomBytes = Array.ofDim[Byte](IdSize)
    random.nextBytes(randomBytes)
    val attempt = Files.createTempDirectory(getClass.getSimpleName)
    Files.write(attempt.resolve("id"), randomBytes)
    retry { (_, _) =>
      Future {
        Files.move(attempt, location, StandardCopyOption.ATOMIC_MOVE)
        val writtenBytes = Files.readAllBytes(location.resolve("id"))
        //noinspection CorrespondsUnsorted
        if (!writtenBytes.sameElements(randomBytes)) {
          throw new AcquisitionFailedException
        }
      }
    }
  }

  private def release()(implicit executionContext: ExecutionContext): Future[Unit] =
    Future {
      deleteFiles(location)
    }
}

object FileSystemLock {
  private val IdSize: Int = 16

  private val retry: RetryStrategy =
    RetryStrategy.constant(attempts = Some(1000), waitTime = 10.millis) {
      case _: FileSystemException => true
      case _: AcquisitionFailedException => true
    }

  class AcquisitionFailedException extends RuntimeException("File system lock acquisition failed.")
}
