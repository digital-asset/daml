// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import org.scalatest.TryValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class UniversalArchiveReaderSpec extends AnyFlatSpec with Matchers with TryValues {

  private val darFile = new File(rlocation("daml-lf/archive/DarReaderTest.dar"))

  private val dalfFile = new File(rlocation("daml-lf/archive/DarReaderTest.dalf"))

  behavior of UniversalArchiveReader.toString

  it should "parse a DAR file" in {
    UniversalArchiveReader.readFile(darFile) shouldBe a[Right[_, _]]
  }

  it should "parse a DALF file" in {
    UniversalArchiveReader.readFile(dalfFile) shouldBe a[Right[_, _]]
  }

  it should "parse a DAR file and return language version" in {
    UniversalArchiveReader.readFile(darFile) shouldBe a[Right[_, _]]
  }

  it should "parse a DALF file and return language version" in {
    UniversalArchiveReader.readFile(dalfFile) shouldBe a[Right[_, _]]
  }

  it should "reject a zip bomb with the proper error" in {
    UniversalArchiveReader
      .readFile(darFile, entrySizeThreshold = 1024) shouldBe Left(Error.ZipBomb)
  }

}
