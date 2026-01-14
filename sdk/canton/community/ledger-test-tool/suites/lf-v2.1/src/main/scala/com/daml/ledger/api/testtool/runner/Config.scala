// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.runner

import com.daml.ledger.api.testtool.infrastructure.PartyAllocationConfiguration
import com.daml.ledger.api.testtool.runner
import com.digitalasset.canton.config.TlsClientConfig

import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

final case class Config(
    jsonApiMode: Boolean,
    participantsEndpoints: Vector[(String, Int)],
    participantsAdminEndpoints: Vector[(String, Int)],
    maxConnectionAttempts: Int,
    mustFail: Boolean,
    verbose: Boolean,
    timeoutScaleFactor: Double,
    concurrentTestRuns: Int,
    extract: Boolean,
    tlsConfig: Option[TlsClientConfig],
    excluded: Set[String],
    included: Set[String],
    additional: Set[String],
    listTests: Boolean,
    listTestSuites: Boolean,
    shuffleParticipants: Boolean,
    partyAllocation: PartyAllocationConfiguration,
    ledgerClockGranularity: FiniteDuration,
    skipDarNamesPattern: Option[Regex],
    reportOnFailuresOnly: Boolean,
    connectedSynchronizers: Int,
) {
  def withTlsConfig(modify: TlsClientConfig => TlsClientConfig): Config = {
    val base = tlsConfig.getOrElse(TlsClientConfig(None, None))
    copy(tlsConfig = Some(modify(base)))
  }
}

object Config {
  val DefaultTestDarExclusions = new Regex(
    ".*upgrade-tests.*|ongoing-stream-package-upload-tests.*"
  )

  val default: Config = Config(
    jsonApiMode = false,
    participantsEndpoints = Vector.empty,
    participantsAdminEndpoints = Vector.empty,
    maxConnectionAttempts = 10,
    mustFail = false,
    verbose = false,
    timeoutScaleFactor = Defaults.TimeoutScaleFactor,
    concurrentTestRuns = runner.Defaults.ConcurrentRuns,
    extract = false,
    tlsConfig = None,
    excluded = Set.empty,
    included = Set.empty,
    additional = Set.empty,
    listTests = false,
    listTestSuites = false,
    shuffleParticipants = false,
    partyAllocation = PartyAllocationConfiguration.ClosedWorldWaitingForAllParticipants,
    ledgerClockGranularity = runner.Defaults.LedgerClockGranularity,
    skipDarNamesPattern = Some(DefaultTestDarExclusions),
    reportOnFailuresOnly = false,
    connectedSynchronizers = 1,
  )
}
