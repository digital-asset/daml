// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.util.BinaryFileUtil
import com.google.protobuf.ByteString

import java.io.File
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission.{OWNER_READ, OWNER_WRITE}
import scala.jdk.CollectionConverters.*

package object commands {

  /** Runs every body, even if some of them fail with a `CommandExecutionFailedException`.
    * Succeeds, if all bodies succeed.
    * If some body throws a `Throwable` other than `CommandExecutionFailedException`, the execution terminates immediately with that exception.
    * If some body throws a `CommandExecutionFailedException`, subsequent bodies are still executed and afterwards the
    * methods throws a `CommandExecutionFailedException`, preferring `CantonInternalErrors` over `CommandFailure`.
    */
  private[commands] def runEvery[A](bodies: Seq[() => Unit]): Unit = {
    val exceptions = bodies.mapFilter(body =>
      try {
        body()
        None
      } catch {
        case e: CommandFailure => Some(e)
        case e: CantonInternalError => Some(e)
      }
    )
    // It is ok to discard all except one exceptions, because:
    // - The exceptions do not have meaningful messages. Error messages are logged instead.
    // - The exception have all the same stack trace.
    exceptions.collectFirst { case e: CantonInternalError => throw e }.discard
    exceptions.headOption.foreach(throw _)
  }

  private[commands] def timestampFromInstant(
      instant: java.time.Instant
  )(implicit loggingContext: ErrorLoggingContext): CantonTimestamp =
    CantonTimestamp.fromInstant(instant).valueOr { err =>
      loggingContext.logger.error(err)(loggingContext.traceContext)
      throw new CommandFailure()
    }

  private[commands] def writeToFile(outputFile: String, bytes: ByteString): Unit = {
    val file = new File(outputFile)
    file.createNewFile()
    // only current user has permissions with the file
    try {
      Files.setPosixFilePermissions(file.toPath, Set(OWNER_READ, OWNER_WRITE).asJava)
    } catch {
      // the above will throw on non-posix systems such as windows
      case _: UnsupportedOperationException =>
    }
    BinaryFileUtil.writeByteStringToFile(outputFile, bytes)
  }
}
