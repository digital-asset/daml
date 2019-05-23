// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.config

import com.daml.ledger.participant.state.index.v1.PackagesService
import com.digitalasset.platform.sandbox.cli.Cli
import com.digitalasset.platform.sandbox.damle.SandboxTemplateStore

case class SandboxContext(
    config: SandboxConfig,
    packageService: PackagesService,
    packageContainer: DamlPackageContainer)

object SandboxContext {
  def apply(args: Array[String]): Option[SandboxContext] =
    Cli.parse(args).map(fromConfig)

  def fromConfig(config: SandboxConfig): SandboxContext = {
    val ts = SandboxTemplateStore(config.damlPackageContainer)
    SandboxContext(config, ts, config.damlPackageContainer)
  }

  /** Parses the arguments into a SandboxContext object. In case of failure calls System.exit()! */
  def havingContext[T](args: Array[String], onContext: SandboxContext => T): T =
    apply(args).fold(sys.exit(1))(onContext)

}
