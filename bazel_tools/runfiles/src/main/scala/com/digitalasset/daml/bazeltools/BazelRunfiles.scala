// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.bazeltools

import java.nio.file.{Path, Paths}

import com.google.devtools.build.runfiles.Runfiles

trait BazelRunfiles {
  private val MainWorkspace = "com_github_digital_asset_daml"

  private val MainWorkspacePath = Paths.get(MainWorkspace)

  private val inBazelEnvironment =
    Set("RUNFILES_DIR", "JAVA_RUNFILES", "RUNFILES_MANIFEST_FILE", "RUNFILES_MANIFEST_ONLY").exists(
      sys.env.contains)

  def rlocation(path: String): String =
    if (inBazelEnvironment)
      Runfiles.create.rlocation(MainWorkspace + "/" + path)
    else
      path

  def rlocation(path: Path): Path =
    if (inBazelEnvironment) {
      val workspacePathString = MainWorkspacePath
        .resolve(path)
        .toString
        .replaceAllLiterally("\\", "/")
      val runfilePath = Option(Runfiles.create.rlocation(workspacePathString))
      Paths.get(runfilePath.getOrElse(throw new IllegalArgumentException(path.toString)))
    } else
      path
}

object BazelRunfiles extends BazelRunfiles
