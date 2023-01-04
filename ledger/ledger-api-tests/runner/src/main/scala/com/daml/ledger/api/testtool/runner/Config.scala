// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.runner

import com.daml.ledger.api.testtool.infrastructure.PartyAllocationConfiguration
import com.daml.ledger.api.testtool.runner
import com.daml.ledger.api.tls.TlsConfiguration

import scala.concurrent.duration.FiniteDuration

final case class Config(
    participantsEndpoints: Vector[(String, Int)],
    maxConnectionAttempts: Int,
    mustFail: Boolean,
    verbose: Boolean,
    timeoutScaleFactor: Double,
    concurrentTestRuns: Int,
    extract: Boolean,
    tlsConfig: Option[TlsConfiguration],
    excluded: Set[String],
    included: Set[String],
    additional: Set[String],
    listTests: Boolean,
    listTestSuites: Boolean,
    shuffleParticipants: Boolean,
    partyAllocation: PartyAllocationConfiguration,
    ledgerClockGranularity: FiniteDuration,
    uploadDars: Boolean,
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
    uploadDars = true,
  )
}
