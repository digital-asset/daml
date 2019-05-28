// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.platform.sandbox.config.DamlPackageContainer

final case class Config(
    host: String,
    port: Int,
    packageContainer: DamlPackageContainer,
    mustFail: Boolean,
    verbose: Boolean,
    timeoutScaleFactor: Double,
    extract: Boolean,
    tlsConfig: Option[TlsConfiguration]
)

object Config {
  val default = Config(
    host = "localhost",
    port = 6865,
    packageContainer = DamlPackageContainer(),
    mustFail = false,
    verbose = false,
    timeoutScaleFactor = 1.0,
    extract = false,
    tlsConfig = None
  )
}