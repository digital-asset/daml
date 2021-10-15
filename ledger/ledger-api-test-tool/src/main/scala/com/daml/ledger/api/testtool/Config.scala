// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import com.daml.ledger.api.testtool.infrastructure.PartyAllocationConfiguration
import com.daml.ledger.api.tls.TlsConfiguration

import java.io.File
import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration

final case class Config(
    participantsEndpoints: Vector[(String, Int)],
    maxConnectionAttempts: Int,
    darPackages: List[File],
    mustFail: Boolean,
    verbose: Boolean,
    timeoutScaleFactor: Double,
    concurrentTestRuns: Int,
    extract: Boolean,
    tlsConfig: Option[TlsConfiguration],
    excluded: Set[String],
    included: Set[String],
    additional: Set[String],
    performanceTests: Set[String],
    performanceTestsReport: Option[Path],
    listTests: Boolean,
    listTestSuites: Boolean,
    shuffleParticipants: Boolean,
    partyAllocation: PartyAllocationConfiguration,
    ledgerClockGranularity: FiniteDuration,
    uploadDars: Boolean,
    staticTime: Boolean,
) {
  def withTlsConfig(modify: TlsConfiguration => TlsConfiguration): Config = {
    val base = tlsConfig.getOrElse(TlsConfiguration.Empty)
    copy(tlsConfig = Some(modify(base)))
  }
}

object Config {
  val default: Config = Config(
    participantsEndpoints = Vector.empty,
    maxConnectionAttempts = 10,
    darPackages = Nil,
    mustFail = false,
    verbose = false,
    timeoutScaleFactor = tests.Defaults.TimeoutScaleFactor,
    concurrentTestRuns = tests.Defaults.ConcurrentRuns,
    extract = false,
    tlsConfig = None,
    excluded = Set.empty,
    included = Set.empty,
    additional = Set.empty,
    performanceTests = Set.empty,
    performanceTestsReport = None,
    listTests = false,
    listTestSuites = false,
    shuffleParticipants = false,
    partyAllocation = PartyAllocationConfiguration.ClosedWorldWaitingForAllParticipants,
    ledgerClockGranularity = tests.Defaults.LedgerClockGranularity,
    uploadDars = true,
    staticTime = false,
  )
}
