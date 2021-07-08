// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.testing

import java.util.zip.ZipInputStream

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.test.TestDar
import com.daml.lf.archive.{Dar, RawDarReader}

import scala.util.Try

object TestDarReader {

  def readCommonTestDar(testDar: TestDar): Try[Dar[DamlLf.Archive]] = read(testDar.path)

  def read(path: String): Try[Dar[DamlLf.Archive]] = {
    RawDarReader
      .readArchive(
        path,
        new ZipInputStream(this.getClass.getClassLoader.getResourceAsStream(path)),
      )
  }
}
