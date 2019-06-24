// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.bazeltools

import com.google.devtools.build.runfiles.Runfiles

trait BazelRunfiles {

  private val MainWorkspace = "com_github_digital_asset_daml"

  private val inBazelEnvironment =
    Set("RUNFILES_DIR", "JAVA_RUNFILES", "RUNFILES_MANIFEST_FILE", "RUNFILES_MANIFEST_ONLY").exists(
      sys.env.contains)

  def rlocation(path: String): String =
    if (inBazelEnvironment) Runfiles.create.rlocation(MainWorkspace + "/" + path) else path

}

object BazelRunfiles extends BazelRunfiles
