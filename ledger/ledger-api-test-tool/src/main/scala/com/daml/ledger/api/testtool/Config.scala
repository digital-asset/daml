// Copyright (c) 2020 The DAML Authors. All rights reserved.
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
    loadScaleFactor: Double,
    concurrentTestRuns: Int,
    extract: Boolean,
    tlsConfig: Option[TlsConfiguration],
    excluded: Set[String],
    included: Set[String],
    listTests: Boolean,
    allTests: Boolean,
    waitForParties: Boolean,
    shuffleParticipants: Boolean,
    openWorld: Boolean,
)

object Config {
  val default: Config = Config(
    participants = Vector.empty,
    darPackages = Nil,
    mustFail = false,
    verbose = false,
    commandSubmissionTtlScaleFactor = 1.0,
    timeoutScaleFactor = 1.0,
    loadScaleFactor = 1.0,
    concurrentTestRuns = Runtime.getRuntime.availableProcessors(),
    extract = false,
    tlsConfig = None,
    excluded = Set.empty,
    included = Set.empty,
    listTests = false,
    allTests = false,
    waitForParties = true,
    shuffleParticipants = false,
    openWorld = false,
  )
}
