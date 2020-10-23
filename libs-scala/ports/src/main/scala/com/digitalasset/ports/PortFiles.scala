// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import java.nio.file.{Files, Path}

import scalaz.{Show, \/}

import scala.collection.JavaConverters._

object PortFiles {
  sealed abstract class Error extends Serializable with Product
  final case class CannotWriteToFile(path: Path, reason: String) extends Error

  object Error {
    implicit val showInstance: Show[Error] = Show.shows {
      case CannotWriteToFile(path, reason) =>
        s"Cannot write to port file: ${path.toAbsolutePath: Path}, reason: $reason"
    }
  }

  /**
    * Creates a port file and requests that the created file be deleted when the virtual machine terminates.
    * See [[java.io.File#deleteOnExit()]].
    */
  def write(path: Path, port: Port): Error \/ Unit =
    \/.fromTryCatchNonFatal {
      writeUnsafe(path, port)
    }.leftMap {
      case e => CannotWriteToFile(path, e.toString)
    }

  private def writeUnsafe(path: Path, port: Port): Unit = {
    import java.nio.file.StandardOpenOption.WRITE
    val lines: java.lang.Iterable[String] = List(port.value.toString).asJava
    val created = Files.write(path, lines, WRITE)
    created.toFile.deleteOnExit()
  }
}
