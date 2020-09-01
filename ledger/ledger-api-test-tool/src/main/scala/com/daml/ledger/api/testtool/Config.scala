// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.io.File
import java.nio.file.Path

import com.daml.ledger.api.testtool.infrastructure.PartyAllocationConfiguration
import com.daml.ledger.api.tls.TlsConfiguration

final case class Config(
    participants: Vector[(String, Int)],
    darPackages: List[File],
    mustFail: Boolean,
    verbose: Boolean,
    timeoutScaleFactor: Double,
    concurrentTestRuns: Int,
    extract: Boolean,
    tlsConfig: Option[TlsConfiguration],
    excluded: Set[String],
    included: Set[String],
    performanceTests: Set[String],
    performanceTestsReport: Option[Path],
    listTests: Boolean,
    listTestSuites: Boolean,
    shuffleParticipants: Boolean,
    partyAllocation: PartyAllocationConfiguration,
    ledgerClockTickIntervalMs: Int
)

object Config {
  val default: Config = Config(
    participants = Vector.empty,
    darPackages = Nil,
    mustFail = false,
    verbose = false,
    timeoutScaleFactor = 1.0,
    concurrentTestRuns = Runtime.getRuntime.availableProcessors(),
    extract = false,
    tlsConfig = None,
    excluded = Set.empty,
    included = Set.empty,
    performanceTests = Set.empty,
    performanceTestsReport = None,
    listTests = false,
    listTestSuites = false,
    shuffleParticipants = false,
    partyAllocation = PartyAllocationConfiguration.ClosedWorldWaitingForAllParticipants,
    ledgerClockTickIntervalMs = 10000
  )
}
