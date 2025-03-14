// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.util.Using

/** Utility methods for loading resource test files.
  */
object JarResourceUtils {
  // Extract a resource and store it in a temporary file.
  // This method works for resources embedded in a JAR.
  def extractFileFromJar(path: String): File =
    Using(getClass.getClassLoader.getResourceAsStream(path)) { inputStream =>
      if (inputStream == null) throw new RuntimeException(s"Resource for $path not found")

      // In case of absolute path, get only the file name
      // (to be used as temp file name)
      val tmpFileName = new File(path).getName
      val tmpFilePath = Files.createTempFile(tmpFileName, "tmp")
      Files.copy(inputStream, tmpFilePath, StandardCopyOption.REPLACE_EXISTING)
      tmpFilePath.toFile
    }.getOrElse(throw new RuntimeException(s"Resource for $path not found"))

  // Use resource file directly wrapping it in a File. This method works in unit tests.
  // It doesn't work for resources from a JAR
  def resourceFile(path: String): File =
    Option(getClass.getClassLoader.getResource(path))
      .map(_.toURI)
      .map(Paths.get(_).toFile)
      .getOrElse(throw new RuntimeException(s"Resource for $path not found"))
}
