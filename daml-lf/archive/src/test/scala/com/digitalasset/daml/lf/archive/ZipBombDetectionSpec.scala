// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.io.File
import com.daml.bazeltools.BazelRunfiles
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class ZipBombDetectionSpec extends AnyFlatSpec with Matchers with TryValues {

  private def bomb = new File(BazelRunfiles.rlocation("daml-lf/archive/DarReaderTest.dar"))

  "DarReader" should "reject a zip bomb with the proper error" in {
    DarReader
      .readArchiveFromFile(bomb, entrySizeThreshold = 1024) shouldBe Left(Error.ZipBomb)
  }

  "UniversalArchiveReader" should "reject a zip bomb with the proper error" in {
    UniversalArchiveReader
      .readFile(bomb, entrySizeThreshold = 1024) shouldBe Left(Error.ZipBomb)
  }

}
