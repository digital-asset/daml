// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.generator.app

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.daml.error.generator.ErrorCodeInventoryDocsGenerator

/** Generates error codes inventory as a reStructuredText
  */
object ErrorCodeInventoryDocsGenApp {

  def main(args: Array[String]): Unit = {
    val text = ErrorCodeInventoryDocsGenerator.genText()
    if (args.length >= 1) {
      val outputFile = Paths.get(args(0))
      val _ = Files.write(
        outputFile,
        text.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE_NEW,
      )
    } else {
      println(text)
    }

  }

}
