// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

class DeleteFiles extends SimpleFileVisitor[Path] {
  override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
    val result = super.visitFile(file, attrs)
    if (result == FileVisitResult.CONTINUE) {
      Files.delete(file)
      FileVisitResult.CONTINUE
    } else
      result
  }

  override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
    val result = super.postVisitDirectory(dir, exc)
    if (result == FileVisitResult.CONTINUE) {
      Files.delete(dir)
      FileVisitResult.CONTINUE
    } else
      result
  }
}

object DeleteFiles {
  def deleteFiles(path: Path): Unit = {
    Files.walkFileTree(path, new DeleteFiles)
    ()
  }
}
