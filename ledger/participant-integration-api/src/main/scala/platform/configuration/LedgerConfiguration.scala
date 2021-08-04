// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.time.Duration

import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}

/** Configuration on initializing a ledger, and waiting for it to initialize.
  *
  * @param initialConfiguration     The initial ledger configuration to submit if the ledger does
  *                                 not contain one yet.
  * @param configurationLoadTimeout The amount of time the ledger API server will wait to load a
  *                                 ledger configuration.
  *
  *                                 The ledger API server waits at startup until it reads at least
  *                                 one configuration changed update from the ledger. If none is
  *                                 found within this timeout, the ledger API server will start
  *                                 anyway, but services that depend on the ledger configuration
  *                                 (e.g., `CommandSubmissionService` and `CommandService`) will
  *                                 reject all requests with the `UNAVAILABLE` error.
  */
final case class LedgerConfiguration(
    initialConfiguration: InitialLedgerConfiguration,
    configurationLoadTimeout: Duration,
)

object LedgerConfiguration {
  private val reasonableInitialConfiguration: Configuration = Configuration(
    generation = Configuration.StartingGeneration,
    timeModel = LedgerTimeModel.reasonableDefault,
    maxDeduplicationTime = Duration.ofDays(1),
  )

  /** Default configuration for a ledger-backed index,
    * i.e., if there is zero delay between the ledger and the index.
    * Example: Sandbox classic.
    */
  val defaultLedgerBackedIndex: LedgerConfiguration = LedgerConfiguration(
    initialConfiguration = InitialLedgerConfiguration(
      configuration = reasonableInitialConfiguration,
      delayBeforeSubmitting = Duration.ZERO,
    ),
    configurationLoadTimeout = Duration.ofSeconds(1),
  )

  /** Default configuration for a local single-participant ledger,
    * i.e., if there is minimal delay between the ledger and the index.
    * Example: Sandbox next.
    */
  val defaultLocalLedger: LedgerConfiguration = LedgerConfiguration(
    initialConfiguration = InitialLedgerConfiguration(
      configuration = reasonableInitialConfiguration,
      delayBeforeSubmitting = Duration.ofMillis(500),
    ),
    configurationLoadTimeout = Duration.ofSeconds(5),
  )

  /** Default configuration for a participant connecting to a remote ledger,
    * i.e., if there may be significant delay between the ledger and the index.
    */
  val defaultRemote: LedgerConfiguration = LedgerConfiguration(
    initialConfiguration = InitialLedgerConfiguration(
      configuration = reasonableInitialConfiguration,
      delayBeforeSubmitting = Duration.ofSeconds(5),
    ),
    configurationLoadTimeout = Duration.ofSeconds(10),
  )

  @deprecated("Please use the new constructor.", since = "1.16")
  def apply(
      initialConfiguration: Configuration,
      initialConfigurationSubmitDelay: Duration,
      configurationLoadTimeout: Duration,
  ): LedgerConfiguration =
    apply(
      InitialLedgerConfiguration(initialConfiguration, initialConfigurationSubmitDelay),
      configurationLoadTimeout,
    )
}
