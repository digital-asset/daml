// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import java.io.{File, FileInputStream}
import java.util.zip.ZipInputStream

import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import org.scalatest.{FlatSpec, Matchers, TryValues}

class UniversalArchiveReaderSpec extends FlatSpec with Matchers with TryValues {

  private val darFile = new File(rlocation("daml-lf/archive/DarReaderTest.dar"))

  private val dalfFile = new File(rlocation("daml-lf/archive/DarReaderTest.dalf"))

  private def bomb: ZipInputStream =
    new ZipInputStream(new FileInputStream(BazelRunfiles.rlocation("libs_python/zblg.zip")))

  behavior of classOf[UniversalArchiveReader[_]].getSimpleName

  it should "parse a DAR file" in {
    UniversalArchiveReader().readFile(darFile).success
  }

  it should "parse a DALF file" in {
    UniversalArchiveReader().readFile(dalfFile).success
  }

  it should "parse a DAR file and return language version" in {
    UniversalArchiveReaderWithVersion().readFile(darFile).success
  }

  it should "parse a DALF file and return language version" in {
    UniversalArchiveReaderWithVersion().readFile(dalfFile).success
  }

  it should "reject a zip bomb with the proper error" in {
    DarReader().readArchive("t", bomb).failure.exception shouldBe a[Errors.ZipBomb]
  }

}
