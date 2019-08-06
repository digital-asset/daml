// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.io.File

import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.archive.UniversalArchiveReader
import com.digitalasset.platform.testing.TestTemplateIdentifiers

package object templates {

  private[this] val dar = new File(BazelRunfiles.rlocation("ledger/test-common/Test.dar"))
  private[this] val parsedPackageId: String = UniversalArchiveReader().readFile(dar).get.main._1
  val ids = new TestTemplateIdentifiers(parsedPackageId)

}
