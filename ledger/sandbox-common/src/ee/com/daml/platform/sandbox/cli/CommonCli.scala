// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.cli

import java.io.File

import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import scopt.OptionParser

class CommonCli(name: LedgerName) extends CommonCliBase(name) {
  override val parser: OptionParser[SandboxConfig] = {
    val parser = super.parser
    parser
      .opt[File]("profile-dir")
      .optional()
      .action((dir, config) => config.copy(profileDir = Some(dir.toPath)))
      .text("Enable profiling and write the profiles into the given directory.")
    parser
  }
}
