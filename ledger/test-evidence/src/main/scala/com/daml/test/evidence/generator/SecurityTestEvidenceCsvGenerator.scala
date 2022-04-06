// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.generator

import com.daml.test.evidence.generator.TestEntryCsvEncoder.SecurityTestEntryCsv

import java.nio.file.{Files, Paths, StandardOpenOption}

object SecurityTestEvidenceCsvGenerator {

  def genText(): String =
    TestEntryCsvEncoder.generateOutput(
      TestEntryLookup.securityTestEntries.map(SecurityTestEntryCsv.apply)
    )

  def main(args: Array[String]): Unit = {
    if (args.length >= 1) {
      val outputFile = Paths.get(args(0))
      val outputText = genText()
      Files.write(outputFile, outputText.getBytes, StandardOpenOption.CREATE_NEW): Unit
    } else {
      val outputText = genText()
      println(outputText)
    }
    sys.exit()
  }
}
