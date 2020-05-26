// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import java.io.File
import java.nio.file.Files

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.DarReader
import com.daml.lf.codegen.backend.java.JavaBackend
import com.daml.lf.codegen.conf.Conf
import org.scalatest.{FlatSpec, Matchers}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class CodeGenRunnerTests extends FlatSpec with Matchers with BazelRunfiles {

  behavior of "collectDamlLfInterfaces"

  def path(p: String) = new File(p).getAbsoluteFile.toPath

  val testDar = path(rlocation("language-support/java/codegen/test-daml.dar"))
  val dar = DarReader().readArchiveFromFile(testDar.toFile).get

  val dummyOutputDir = Files.createTempDirectory("codegen")

  it should "always use JavaBackend, which is currently hardcoded" in {
    CodeGenRunner.backend should be theSameInstanceAs JavaBackend
  }

  it should "read interfaces from a single DAR file without a prefix" in {

    val conf = Conf(
      Map(testDar -> None),
      dummyOutputDir,
    )

    val (interfaces, pkgPrefixes) = CodeGenRunner.collectDamlLfInterfaces(conf)

    assert(interfaces.length == 19)
    assert(pkgPrefixes == Map.empty)
  }

  it should "read interfaces from a single DAR file with a prefix" in {

    val conf = Conf(
      Map(testDar -> Some("PREFIX")),
      dummyOutputDir,
    )

    val (interfaces, pkgPrefixes) = CodeGenRunner.collectDamlLfInterfaces(conf)

    assert(interfaces.map(_.packageId).length == dar.all.length)
    assert(pkgPrefixes.size == dar.all.length)
    assert(pkgPrefixes.values.forall(_ == "PREFIX"))
  }
}
