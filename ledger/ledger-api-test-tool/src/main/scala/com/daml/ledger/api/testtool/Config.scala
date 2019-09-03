// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.io.File

import com.digitalasset.ledger.api.tls.TlsConfiguration

final case class Config(
    participants: Vector[(String, Int)],
    darPackages: List[File],
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
    participants = Vector.empty,
    darPackages = Nil,
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
