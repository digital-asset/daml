// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.io.FileInputStream
import java.util.zip.ZipInputStream

import com.daml.bazeltools.BazelRunfiles
import org.scalatest.{FlatSpec, Matchers, TryValues}

final class ZipBombDetectionSpec extends FlatSpec with Matchers with TryValues {

  private def bomb: ZipInputStream =
    new ZipInputStream(
      new FileInputStream(BazelRunfiles.rlocation("daml-lf/archive/DarReaderTest.dar"))
    )

  "DarReader" should "reject a zip bomb with the proper error" in {
    DarReader()
      .readArchive("t", bomb, entrySizeThreshold = 1024)
      .failure
      .exception shouldBe a[Errors.ZipBomb]
  }

  "UniversalArchiveReader" should "reject a zip bomb with the proper error" in {
    UniversalArchiveReader(entrySizeThreshold = 1024)
      .readDarStream("t", bomb)
      .failure
      .exception shouldBe a[Errors.ZipBomb]
  }

}
