// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive

import com.daml.bazeltools.BazelRunfiles
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File

class LegacyArchiveReaderTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with BazelRunfiles
    with Inspectors
    with TryValues {

  private def resource(path: String): File = {
    val f = new File(rlocation(path)).getAbsoluteFile
    require(f.exists, s"File does not exist: $f")
    f
  }

    "Legacy archive readers should fail with mutated 2.1 code" in {
      val darFile = resource("daml-lf/archive/DarReaderTest.dar")
      inside(DarDecoder.readArchiveFromFile(darFile)) { case Left(err) =>
        err.msg should include("BuiltinFunction.UNRECOGNIZED")
      }
    }

}
