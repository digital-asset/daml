// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.io.RandomAccessFile
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Path

import com.daml.ledger.on.filesystem.posix.FileSystemLock._
import com.digitalasset.timer.RetryStrategy

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class FileSystemLock(location: Path) {
  def apply[T](body: => T)(implicit executionContext: ExecutionContext): Future[T] = {
    val file = new RandomAccessFile(location.toFile, "rw")
    val channel = file.getChannel
    retry { (_, _) =>
      Future(channel.lock())
        .flatMap { lock =>
          Future(body).transformWith {
            case Success(value) =>
              lock.close()
              Future.successful(value)
            case Failure(exception) =>
              lock.close()
              Future.failed(exception)
          }
        }
    }
  }
}

object FileSystemLock {
  private val retry: RetryStrategy =
    RetryStrategy.constant(attempts = Some(1000), waitTime = 10.millis) {
      case _: OverlappingFileLockException => true
    }
}
