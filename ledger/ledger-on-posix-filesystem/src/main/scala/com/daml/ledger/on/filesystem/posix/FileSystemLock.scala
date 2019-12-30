// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.{FileAlreadyExistsException, Files, Path, StandardCopyOption}

import com.daml.ledger.on.filesystem.posix.DeleteFiles.deleteFiles
import com.daml.ledger.on.filesystem.posix.FileSystemLock._
import com.digitalasset.timer.RetryStrategy

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random
import scala.util.control.NonFatal

class FileSystemLock(location: Path) {
  private val random: Random = new Random()

  def run[T](body: => Future[T])(implicit executionContext: ExecutionContext): Future[T] =
    for {
      _ <- acquire()
      result <- body
    } yield {
      release()
      result
    }

  private def acquire()(implicit executionContext: ExecutionContext): Future[Unit] = {
    val randomBytes = Array.ofDim[Byte](IdSize)
    random.nextBytes(randomBytes)
    retryStrategy(
      (_, _) =>
        Future {
          val attempt = Files.createTempDirectory(getClass.getSimpleName)
          Files.write(attempt.resolve("id"), randomBytes)
          try {
            Files.move(attempt, location, StandardCopyOption.ATOMIC_MOVE)
            Some(Files.readAllBytes(location.resolve("id")))
          } catch {
            case _: FileAlreadyExistsException =>
              deleteFiles(attempt)
              None
            case NonFatal(exception) =>
              deleteFiles(attempt)
              throw exception
          }
        }.flatMap {
          case None =>
            Future.failed(new AcquisitionFailedException)
          case Some(writtenBytes) =>
            //noinspection CorrespondsUnsorted
            if (writtenBytes.sameElements(randomBytes)) {
              Future.successful(())
            } else {
              Future.failed(new AcquisitionFailedException)
            }
      }
    )
  }

  private def release()(implicit executionContext: ExecutionContext): Future[Unit] =
    Future {
      deleteFiles(location)
    }
}

object FileSystemLock {
  private val IdSize: Int = 16

  private val retryStrategy: RetryStrategy =
    RetryStrategy.constant(attempts = 1000, waitTime = 10.millis)

  class AcquisitionFailedException extends RuntimeException("File system lock acquisition failed.")
}
