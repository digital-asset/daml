// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.config

import com.digitalasset.platform.sandbox.cli.Cli
import com.digitalasset.platform.sandbox.damle.SandboxTemplateStore

case class SandboxContext(
    config: SandboxConfig,
    sandboxTemplateStore: SandboxTemplateStore,
    packageContainer: DamlPackageContainer)

object SandboxContext {
  def apply(args: Array[String]): Option[SandboxContext] =
    Cli.parse(args).map(fromConfig)

  def fromConfig(config: SandboxConfig): SandboxContext = {
    val ts = new SandboxTemplateStore(config.damlPackageContainer)
    SandboxContext(config, ts, config.damlPackageContainer)
  }

  /** Parses the arguments into a SandboxContext object. In case of failure calls System.exit()! */
  def havingContext[T](args: Array[String], onContext: SandboxContext => T): T =
    apply(args).fold(sys.exit(1))(onContext)

}
