// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ports

import java.nio.file.{Files, Path}

import scalaz.{-\/, Show, \/}

object PortFiles {
  sealed abstract class Error extends Serializable with Product
  final case class FileAlreadyExists(path: Path) extends Error
  final case class CannotWriteIntoFile(path: Path, reason: String) extends Error

  object Error {
    implicit val showInstance: Show[Error] = Show.shows {
      case FileAlreadyExists(path) =>
        s"Port file already exists: ${path.toAbsolutePath: Path}"
      case CannotWriteIntoFile(path, reason) =>
        s"Cannot write into port file: ${path.toAbsolutePath: Path}, reason: $reason"
    }
  }

  /**
    * Creates a port and requests that the created file be deleted when the virtual machine terminates.
    * See [[java.io.File#deleteOnExit()]].
    */
  def write(path: Path, port: Port): Error \/ Unit =
    if (path.toFile.exists())
      -\/(FileAlreadyExists(path))
    else
      \/.fromTryCatchNonFatal {
        writeUnsafe(path, port)
      }.leftMap(e => CannotWriteIntoFile(path, e.getMessage))

  private def writeUnsafe(path: Path, port: Port): Unit = {
    import scala.collection.JavaConverters._
    import java.nio.file.StandardOpenOption.CREATE_NEW
    val lines: java.lang.Iterable[String] = List(port.value.toString).asJava
    val created = Files.write(path, lines, CREATE_NEW)
    created.toFile.deleteOnExit()
  }
}
