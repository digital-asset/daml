// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

import java.nio.file.{Files, Paths}

import com.digitalasset.daml.lf.codegen.backend.java.JavaBackend
import com.digitalasset.daml.lf.codegen.conf.Conf
import org.scalatest.FlatSpec

class CodeGenRunnerTests extends FlatSpec {

  behavior of "collectDamlLfInterfaces"

  val testDalf = Paths.get(
    "language-support/java/codegen/src/test/resources/codegenit.dalf"
  )
  val testDar = Paths.get(
    "language-support/java/codegen/src/test/resources/codegenit.dar"
  )
  val testInterfacePackageId = "048b630ff1067b5dd148bc7eafda9031c654ebe2dad126b46f996471b0a3c9b0"
  val testDar2 = Paths.get(
    "language-support/java/codegen/src/test/resources/codegenit2.dar"
  )

  val dummyOutputDir = Files.createTempDirectory("codegen")

  it should "reads interface from DALF file without prefix " in {

    val conf = Conf(
      Map(testDalf -> None),
      dummyOutputDir,
      JavaBackend
    )

    val (interfaces, pkgPrefixes) = CodeGenRunner.collectDamlLfInterfaces(conf)

    assert(interfaces.length == 1)
    assert(
      interfaces.head.packageId.underlyingString == testInterfacePackageId
    )
    assert(pkgPrefixes == Map.empty)
  }

  it should "reads interface from DALF file with prefix " in {

    val conf = Conf(
      Map(testDalf -> Some("PREFIX")),
      dummyOutputDir,
      JavaBackend
    )

    val (interfaces, pkgPrefixes) = CodeGenRunner.collectDamlLfInterfaces(conf)

    assert(interfaces.length == 1)
    assert(
      interfaces.head.packageId.underlyingString == testInterfacePackageId
    )
    assert(pkgPrefixes == Map(interfaces.head.packageId -> "PREFIX"))
  }

  it should "reads dalf interface from DAR file without prefix " in {

    val conf = Conf(
      Map(testDar -> None),
      dummyOutputDir,
      JavaBackend
    )

    val (interfaces, pkgPrefixes) = CodeGenRunner.collectDamlLfInterfaces(conf)

    assert(interfaces.length == 1)
    assert(
      interfaces.head.packageId.underlyingString == testInterfacePackageId
    )
    assert(pkgPrefixes == Map.empty)
  }

  it should "reads dalf interface from DAR file with prefix " in {

    val conf = Conf(
      Map(testDar -> Some("PREFIX")),
      dummyOutputDir,
      JavaBackend
    )

    val (interfaces, pkgPrefixes) = CodeGenRunner.collectDamlLfInterfaces(conf)

    assert(interfaces.length == 1)
    assert(
      interfaces.head.packageId.underlyingString == testInterfacePackageId
    )
    assert(pkgPrefixes == Map(interfaces.head.packageId -> "PREFIX"))
  }

  it should "reads dalf interface from DAR file with newer manifest" in {

    val conf = Conf(
      Map(testDar2 -> None),
      dummyOutputDir,
      JavaBackend
    )

    val (interfaces, pkgPrefixes) = CodeGenRunner.collectDamlLfInterfaces(conf)

    assert(interfaces.length == 2)
    assert(pkgPrefixes == Map.empty)
  }

}
