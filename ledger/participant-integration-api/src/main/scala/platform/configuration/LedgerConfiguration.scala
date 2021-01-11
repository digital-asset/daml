// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.time.Duration

import com.daml.ledger.participant.state.v1.{Configuration, TimeModel}

/** Configuration surrounding ledger parameters.
  *
  * @param initialConfiguration
  *     The initial ledger configuration to submit if the ledger does not contain one yet.
  * @param initialConfigurationSubmitDelay
  *     The delay until the ledger API server tries to submit an initial configuration at startup if none exists.
  * @param configurationLoadTimeout
  *     The amount of time the ledger API server will wait to load a ledger configuration.
  *
  *     The ledger API server waits at startup until it reads at least one configuration changed update from the ledger.
  *     If none is found within this timeout, the ledger API server will start anyway, but services that depend
  *     on the ledger configuration (e.g., SubmissionService or CommandService) will reject all requests with the
  *     UNAVAILABLE error.
  */
case class LedgerConfiguration(
    initialConfiguration: Configuration,
    initialConfigurationSubmitDelay: Duration,
    configurationLoadTimeout: Duration,
)

object LedgerConfiguration {

  val NoGeneration: Long = 0

  val StartingGeneration: Long = 1

  private val reasonableInitialConfiguration: Configuration = Configuration(
    generation = StartingGeneration,
    timeModel = TimeModel.reasonableDefault,
    maxDeduplicationTime = Duration.ofDays(1),
  )

  /** Default configuration for a ledger-backed index,
    * i.e., if there is zero delay between the ledger and the index.
    * Example: Sandbox classic.
    */
  val defaultLedgerBackedIndex: LedgerConfiguration = LedgerConfiguration(
    initialConfiguration = reasonableInitialConfiguration,
    initialConfigurationSubmitDelay = Duration.ZERO,
    configurationLoadTimeout = Duration.ofSeconds(1),
  )

  /** Default configuration for a local single-participant ledger,
    * i.e., if there is minimal delay between the ledger and the index.
    * Example: Sandbox next.
    */
  val defaultLocalLedger: LedgerConfiguration = LedgerConfiguration(
    initialConfiguration = reasonableInitialConfiguration,
    initialConfigurationSubmitDelay = Duration.ofMillis(500),
    configurationLoadTimeout = Duration.ofSeconds(5),
  )

  /** Default configuration for a participant connecting to a remote ledger,
    * i.e., if there may be significant delay between the ledger and the index.
    */
  val defaultRemote: LedgerConfiguration = LedgerConfiguration(
    initialConfiguration = reasonableInitialConfiguration,
    initialConfigurationSubmitDelay = Duration.ofSeconds(5),
    configurationLoadTimeout = Duration.ofSeconds(10),
  )
}
