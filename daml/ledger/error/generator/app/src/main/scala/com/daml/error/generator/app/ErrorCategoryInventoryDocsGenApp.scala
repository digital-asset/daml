// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator.app

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.daml.error.generator.ErrorCategoryInventoryDocsGenerator

/** Generates error categories inventory as a reStructuredText
  */
object ErrorCategoryInventoryDocsGenApp {

  def main(args: Array[String]): Unit = {
    val outputText = ErrorCategoryInventoryDocsGenerator.genText()
    if (args.length >= 1) {
      val outputFile = Paths.get(args(0))
      Files.write(
        outputFile,
        outputText.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE_NEW,
      ): Unit
    } else {
      println(outputText)
    }
  }

}
