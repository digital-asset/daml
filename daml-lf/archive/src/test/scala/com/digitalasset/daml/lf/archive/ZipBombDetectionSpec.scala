// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import java.io.FileInputStream
import java.util.zip.ZipInputStream

import com.digitalasset.daml.bazeltools.BazelRunfiles
import org.scalatest.{FlatSpec, Matchers, TryValues}

final class ZipBombDetectionSpec extends FlatSpec with Matchers with TryValues {

  private def bomb: ZipInputStream =
    new ZipInputStream(new FileInputStream(BazelRunfiles.rlocation("libs_python/zblg.zip")))

  "DarReader" should "reject a zip bomb with the proper error" in {
    DarReader().readArchive("t", bomb).failure.exception shouldBe a[Errors.ZipBomb]
  }

  "UniversalArchiveReader" should "reject a zip bomb with the proper error" in {
    UniversalArchiveReader()
      .readDarStream("t", bomb)
      .failure
      .exception shouldBe a[Errors.ZipBomb]
  }

}
