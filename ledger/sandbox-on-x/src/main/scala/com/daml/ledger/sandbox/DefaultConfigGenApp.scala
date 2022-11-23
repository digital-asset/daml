// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

// Outputs the default config to a file or to the stdout
object DefaultConfigGenApp {

  def main(args: Array[String]): Unit = {
    val text = genText()
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

  def genText(): String = {
    val config = SandboxOnXConfig()
    val text = ConfigRenderer.render(config)
    text
  }
}
