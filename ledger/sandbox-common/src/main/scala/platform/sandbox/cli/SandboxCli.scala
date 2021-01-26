// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.cli

import com.daml.platform.sandbox.config.SandboxConfig
import scopt.OptionParser

trait SandboxCli {

  def defaultConfig: SandboxConfig

  protected def parser: OptionParser[SandboxConfig]

  def parse(args: Array[String]): Option[SandboxConfig] =
    parser.parse(args, defaultConfig)

}
