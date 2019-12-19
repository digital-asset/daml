// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.io.File

import com.digitalasset.daml.lf.archive.UniversalArchiveReader
import com.digitalasset.platform.PlatformApplications
import com.digitalasset.platform.testing.TestTemplateIdentifiers

final class TestTemplateIds(config: PlatformApplications.Config) {
  lazy val defaultDar: File = config.darFiles.head.toFile
  lazy val parsedPackageId: String =
    UniversalArchiveReader().readFile(defaultDar).get.main._1
  lazy val templateIds: TestTemplateIdentifiers = new TestTemplateIdentifiers(parsedPackageId)
}
