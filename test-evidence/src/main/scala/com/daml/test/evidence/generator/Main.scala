// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.test.evidence.generator

import better.files.File
import com.daml.test.evidence.generator.TestEntryCsvEncoder.{SecurityTestEntryCsv, TestEntryCsv}
import io.circe.Encoder
import io.circe.generic.auto._
import io.circe.syntax._
import com.daml.test.evidence.scalatest.JsonCodec.SecurityJson._

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

  private val workspaceRoot: Option[String] = sys.env.get("BUILD_WORKSPACE_DIRECTORY")
  private val targetDir = workspaceRoot.map(File(_)).getOrElse(File.currentWorkingDirectory)

  private def writeEvidenceToCsvFile[TE <: TestEntryCsv](
      fileName: String,
      entries: List[TE],
  ): Unit = {
    println(s"Writing inventory to $fileName...")
    val file = targetDir / fileName
    val path = file.path.toAbsolutePath
    TestEntryCsvEncoder.write(file, entries)
    println(s"Wrote to $path")
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 2) {
      val securityTestEntries = TestEntryLookup.securityTestEntries
      val csvEntries = securityTestEntries.map(SecurityTestEntryCsv.apply)
      val csvFileName = args(0)
      val jsonFileName = args(1)

      //      println("entries: " + securityTestEntries.mkString(", "))
      writeEvidenceToCsvFile(csvFileName, csvEntries)
      writeEvidenceToJsonFile(jsonFileName, securityTestEntries)

      // explicitly stop here, since some suites have started non-daemon threads that prevent JVM shutdown
      sys.exit(0)
    } else {
      throw new IllegalArgumentException(
        s"Invalid number of arguments, was ${args.length}, should be 2"
      )
    }
  }
}
