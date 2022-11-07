// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.bazeltools

import java.nio.file.{Path, Paths}

import com.google.devtools.build.runfiles.Runfiles

trait BazelRunfiles {
  private val MainWorkspace = "com_github_digital_asset_daml"

  private val MainWorkspacePath = Paths.get(MainWorkspace)

  private val inBazelEnvironment =
    Set("RUNFILES_DIR", "JAVA_RUNFILES", "RUNFILES_MANIFEST_FILE", "RUNFILES_MANIFEST_ONLY").exists(
      sys.env.contains
    )

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
        .replace("\\", "/")
      val runfilePath = Option(Runfiles.create.rlocation(workspacePathString))
      Paths.get(runfilePath.getOrElse(throw new IllegalArgumentException(path.toString)))
    } else
      path

  def requiredResource(name: String): java.io.File = {
    val file = new java.io.File(rlocation(name))
    if (file.exists()) file
    else throw new IllegalStateException(s"File does not exist: ${file.getAbsolutePath}")
  }
}

object BazelRunfiles extends BazelRunfiles
