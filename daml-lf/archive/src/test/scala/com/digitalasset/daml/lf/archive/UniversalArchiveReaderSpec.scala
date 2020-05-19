// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import org.scalatest.{FlatSpec, Matchers, TryValues}

class UniversalArchiveReaderSpec extends FlatSpec with Matchers with TryValues {

  private val darFile = new File(rlocation("daml-lf/archive/DarReaderTest.dar"))

  private val dalfFile = new File(rlocation("daml-lf/archive/DarReaderTest.dalf"))

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

}
