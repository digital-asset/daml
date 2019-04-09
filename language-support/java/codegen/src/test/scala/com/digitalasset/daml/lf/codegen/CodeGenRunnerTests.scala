// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

import java.io.File
import java.nio.file.Files

import com.digitalasset.daml.lf.codegen.backend.java.JavaBackend
import com.digitalasset.daml.lf.codegen.conf.Conf
import org.scalatest.FlatSpec

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CodeGenRunnerTests extends FlatSpec {

  behavior of "collectDamlLfInterfaces"

  def path(p: String) = new File(p).getAbsoluteFile.toPath

  val testDar = path("language-support/java/codegen/test-daml.dar")

  val dummyOutputDir = Files.createTempDirectory("codegen")

  it should "read interfaces from a single DAR file without a prefix" in {

    val conf = Conf(
      Map(testDar -> None),
      dummyOutputDir,
      JavaBackend
    )

    val (interfaces, pkgPrefixes) = CodeGenRunner.collectDamlLfInterfaces(conf)

    assert(interfaces.length == 3)
    assert(pkgPrefixes == Map.empty)
  }

  it should "read interfaces from a single DAR file with a prefix" in {

    val conf = Conf(
      Map(testDar -> Some("PREFIX")),
      dummyOutputDir,
      JavaBackend
    )

    val (interfaces, pkgPrefixes) = CodeGenRunner.collectDamlLfInterfaces(conf)

    assert(interfaces.map(_.packageId).length == 3)
    assert(pkgPrefixes.size == 3)
    assert(pkgPrefixes.values.forall(_ == "PREFIX"))
  }
}
