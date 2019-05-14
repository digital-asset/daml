// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.bazeltools.BazelRunfiles

import java.io.File

import org.scalatest.{FlatSpec, Inside, Matchers}

import scala.util.{Success, Try}

class UniversalArchiveReaderSpec extends FlatSpec with Matchers with Inside with BazelRunfiles {

  private val darFile = new File(rlocation("daml-lf/archive/DarReaderTest.dar"))

  private val dalfFile = new File(rlocation("daml-lf/archive/DarReaderTest.dalf"))

  behavior of classOf[UniversalArchiveReader[_]].getSimpleName

  it should "parse a DAR file" in {
    assertSuccess(UniversalArchiveReader().readFile(darFile))
  }

  it should "parse a DALF file" in {
    assertSuccess(UniversalArchiveReader().readFile(dalfFile))
  }

  it should "parse a DAR file and return language version" in {
    assertSuccess(UniversalArchiveReaderWithVersion().readFile(darFile))
  }

  it should "parse a DALF file and return language version" in {
    assertSuccess(UniversalArchiveReaderWithVersion().readFile(dalfFile))
  }

  private def assertSuccess[A](value: Try[Dar[A]]): Unit = {
    inside(value) {
      case Success(Dar(_, _)) =>
    }
  }
}
