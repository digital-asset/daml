// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error.generator.app

import com.digitalasset.canton.error.generator.ErrorCategoryInventoryDocsGenerator

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

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
