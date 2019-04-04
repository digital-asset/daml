// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.perf.util

import java.io.{File, FileInputStream}

import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.lfpackage.Decode
import com.digitalasset.daml_lf.DamlLf.Archive

object DarUtil {
  def getPackageId(dalf: File): PackageId = {
    val archive = Archive.parseFrom(new FileInputStream(dalf))
    val (packageId, _) = Decode.decodeArchive(archive)
    packageId
  }
}
