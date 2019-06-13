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
    commandSubmissionTtlScaleFactor: Double,
    timeoutScaleFactor: Double,
    extract: Boolean,
    tlsConfig: Option[TlsConfiguration],
    excluded: Set[String],
    included: Set[String],
    listTests: Boolean,
    allTests: Boolean
)

object Config {
  val default = Config(
    host = "localhost",
    port = 6865,
    packageContainer = DamlPackageContainer(),
    mustFail = false,
    verbose = false,
    commandSubmissionTtlScaleFactor = 1.0,
    timeoutScaleFactor = 1.0,
    extract = false,
    tlsConfig = None,
    excluded = Set.empty,
    included = Set.empty,
    listTests = false,
    allTests = false
  )
}
