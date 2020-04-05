// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf.util

import java.io.File

import com.daml.lf.archive.UniversalArchiveReader
import com.daml.lf.data.Ref.PackageId

object DarUtil {
  def getPackageId(dalf: File): PackageId = {
    UniversalArchiveReader().readFile(dalf).get.main._1
  }
}
