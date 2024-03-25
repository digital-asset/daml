// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import better.files.File
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.nio.file.Path

@SuppressWarnings(Array("org.wartremover.warts.Var"))
trait HasTempDirectory extends BeforeAndAfterAll { this: Suite =>

  private var tempDirectoryO: Option[TempDirectory] = None

  lazy val tempDirectory: TempDirectory = {
    val directory = TempDirectory(File.newTemporaryDirectory())

    tempDirectoryO = Some(directory)
    directory
  }

  override def afterAll(): Unit = {
    try super.afterAll()
    finally tempDirectoryO.foreach(_.delete())
  }
}

final case class TempDirectory(directory: File) {
  def delete(): Unit = directory.delete(swallowIOExceptions = true)
  def resolve(other: String): Path = directory.path.resolve(other)
  def path: Path = directory.path
  def relativePath: Path = File.temp.relativize(directory)

  def /(other: String): TempDirectory = this.copy(directory / other)
  def toTempFile(filename: String): TempFile = TempFile(this, filename)
}

final case class TempFile private (file: File) {
  override def toString: String = file.toString()

  def path: Path = file.path

  val parentDirectory: File = file.parent
}

object TempFile {
  def apply(tempDirectory: TempDirectory, filename: String): TempFile = TempFile(
    tempDirectory.directory / filename
  )

  def usingTemporaryFile[U](prefix: String = "", suffix: String = "")(f: TempFile => U): Unit =
    File.usingTemporaryFile(prefix = prefix, suffix = suffix) { file =>
      f(TempFile(file))
    }
}
