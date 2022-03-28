// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.generator

import better.files.File
import com.daml.test.evidence.generator.TestEntryCsvEncoder.TestEntryCsv
import com.daml.test.evidence.scalatest.JsonCodec.SecurityJson._
import com.daml.test.evidence.scalatest.JsonCodec.ReliabilityJson._
import io.circe.Encoder
import io.circe.generic.auto._
import io.circe.syntax._

object Main {

  private def writeEvidenceToJsonFile[TE: Encoder](fileName: String, entries: List[TE]): Unit = {
    println(s"Writing inventory to $fileName...")
    val path = File(fileName)
      .write(entries.asJson.spaces2)
      .path
      .toAbsolutePath
      .toString
    println(s"Wrote to $path")
  }

  private def writeEvidenceToJsonFile[TE: Encoder](fileName: String, entries: List[TE]): Unit = {
    println(s"Writing inventory to $fileName...")
    val path = File(fileName)
      .write(entries.asJson.spaces2)
      .path
      .toAbsolutePath
      .toString
    println(s"Wrote to $path")
  }

  private def writeEvidenceToCsvFile[TE <: TestEntryCsv](
                                                          fileName: String,
                                                          entries: List[TE],
                                                        ): Unit = {
    println(s"Writing inventory to $fileName...")
    val file = File(fileName)
    val path = file.path.toAbsolutePath
    TestEntryCsvEncoder.write(file, entries)
    println(s"Wrote to $path")
  }

  def main(args: Array[String]): Unit = {

    val securityTestEntries = TestEntryLookup.securityTestEntries

    val reliabilityTestEntries = TestEntryLookup.reliabilityTestEntries

    writeEvidenceToFile("security-tests.json", securityTestEntries.asJson.spaces2)

    writeEvidenceToFile("reliability-tests.json", reliabilityTestEntries.asJson.spaces2)

    sys.exit()
  }
}
