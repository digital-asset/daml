// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.conformance

import com.digitalasset.transcode.conformance.data.All
import com.digitalasset.transcode.conformance.generator.DamlRoundtrip

/** Run this to update the Conformance.daml file in daml-examples */
object GenerateDamlTestDefinition:
  def main(args: Array[String]): Unit = {
    val targetDir =
      os.pwd / "community" / "transcode" / "daml-examples" / "src" / "main" / "daml" / "examples"
    println(s"Generating into $targetDir")
    DamlRoundtrip
      .generate(All)
      .foreach((path, contents) => os.write.over(targetDir / path, contents, createFolders = true))
  }
