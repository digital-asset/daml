// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.cli

import java.io.File

import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import scopt.OptionParser

class CommonCli(name: LedgerName) extends CommonCliBase(name) {
  override val parser: OptionParser[SandboxConfig] = {
    val parser = super.parser
    // TODO Remove this once the EE artifacts are consumable for users. Until then
    // we keep it as a hidden option.
    parser
      .opt[File]("profile-dir")
      .optional()
      .hidden()
      .action((dir, config) => config.copy(profileDir = Some(dir.toPath)))
      .text("Enable profiling and write the profiles into the given directory.")
    parser
  }
}
