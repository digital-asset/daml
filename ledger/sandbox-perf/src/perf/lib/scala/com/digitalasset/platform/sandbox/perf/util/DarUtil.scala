// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.perf.util

import java.io.File

import com.digitalasset.daml.lf.archive.UniversalArchiveReader
import com.digitalasset.daml.lf.data.Ref.PackageId

object DarUtil {
  def getPackageId(dalf: File): PackageId = {
    UniversalArchiveReader().readFile(dalf).get.main._1
  }
}
