// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import java.io.RandomAccessFile
import java.nio.channels.{
  ClosedChannelException,
  FileChannel,
  FileLock,
  OverlappingFileLockException
}
import java.nio.file.{Files, Path, Paths}

import com.daml.ports.Port

private[postgresql] object PortLock {

  // We can't use `sys.props("java.io.tmpdir")` because Bazel changes this for each test run.
  // For this to be useful, it needs to be shared across concurrent runs.
  private val portLockDirectory: Path = {
    val tempDirectory =
      if (sys.props("os.name").startsWith("Windows")) {
        Paths.get(sys.props("user.home"), "AppData", "Local", "Temp")
      } else {
        Paths.get("/tmp")
      }
    tempDirectory.resolve(Paths.get("daml", "build", "postgresql-testing", "ports"))
  }

  def lock(port: Port): Either[FailedToLock, Locked] = {
    Files.createDirectories(portLockDirectory)
    val portLockFile = portLockDirectory.resolve(port.toString)
    val file = new RandomAccessFile(portLockFile.toFile, "rw")
    val channel = file.getChannel
    try {
      val lock = channel.tryLock()
      val locked = new Locked(port, lock, channel, file)
      if (lock != null) {
        Right(locked)
      } else {
        locked.unlock()
        Left(FailedToLock(port))
      }
    } catch {
      case _: OverlappingFileLockException =>
        channel.close()
        file.close()
        Left(FailedToLock(port))
    }
  }

  final class Locked(val port: Port, lock: FileLock, channel: FileChannel, file: RandomAccessFile) {
    def unlock(): Unit = {
      try {
        lock.release()
      } catch {
        // ignore
        case _: ClosedChannelException =>
      }
      channel.close()
      file.close()
    }
  }

  case class FailedToLock(port: Port) extends RuntimeException(s"Failed to lock port $port.")

}
