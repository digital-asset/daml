// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import better.files.{DefaultCharset, File}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.Port
import org.slf4j.{Logger, LoggerFactory}

import java.nio.ByteBuffer
import java.nio.channels.{FileLock, OverlappingFileLockException}
import java.nio.charset.StandardCharsets
import scala.concurrent.blocking
import scala.util.{Failure, Success, Try}

/** Generates host-wide unique ports for canton tests that we guarantee won't be used in our tests.
  * Syncs with other processes' UniquePortGenerators via a file + exclusive file lock.
  * Doesn't check that the port hasn't been bound by other processes on the host.
  */
object UniquePortGenerator {
  val PortRangeStart: Int = 30000
  val PortRangeEnd: Int = 65535
  val SharedPortNumFile: File = File.temp / "canton_test_unique_port_generator.txt"

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  SharedPortNumFile.createFileIfNotExists(createParents = true)
  logger.debug(s"Initialized port file: ${SharedPortNumFile.path.toString}")

  private def wrap(port: Int): Int = {
    if (port > PortRangeEnd) {
      PortRangeStart
    } else {
      port
    }
  }

  @annotation.tailrec
  def retryLock[T](retries: Int, sleepMs: Long)(fct: => T): T = {
    Try {
      fct
    } match {
      case Success(x) => x
      case Failure(e: OverlappingFileLockException) if retries > 1 =>
        logger.debug("Failed to acquire lock: ", e)
        Threading.sleep(sleepMs)
        retryLock(retries - 1, sleepMs)(fct)
      case Failure(e) => throw e
    }
  }
  private def exclusiveReadAndWriteIncrement: Int = {
    SharedPortNumFile.usingLock(File.RandomAccessMode.readWriteContentSynchronous) { channel =>
      val lock: FileLock = retryLock(100, 100) {
        blocking(synchronized(channel.lock()))
      }

      try {
        val in = ByteBuffer.allocate(256)
        val len = channel.read(in)
        val port =
          if (len <= 0) PortRangeStart
          else {
            val portStr = new String(in.array(), 0, len, StandardCharsets.UTF_8)
            portStr.toInt
          }
        val out = ByteBuffer.wrap(wrap(port + 1).toString.getBytes(DefaultCharset))
        channel.truncate(0)
        channel.position(0)
        channel.write(out)
        channel.force(true)
        logger.debug(s"Allocated port: $port")
        port
      } finally {
        if (lock.isValid) {
          lock.release()
        }
      }
    }
  }

  def next: Port = Port.tryCreate(exclusiveReadAndWriteIncrement)
}
