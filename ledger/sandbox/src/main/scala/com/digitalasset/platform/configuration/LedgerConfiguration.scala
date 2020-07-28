// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.time.Duration

import com.daml.ledger.participant.state.v1.{Configuration, TimeModel}

/**
  * Configuration surrounding ledger parameters.
  */
case class LedgerConfiguration(
    /**
      * The initial ledger configuration to submit if the ledger does not contain one yet.
      */
    initialConfiguration: Configuration,
    /**
      * The delay until the ledger API server tries to submit an initial configuration at startup if none exists.
      */
    initialConfigurationSubmitDelay: Duration,
    /**
      * The amount of time the ledger API server will wait to load a ledger configuration.
      *
      * The ledger API server waits at startup until it reads at least one configuration changed update from the ledger.
      * If none is found within this timeout, the ledger API server will start anyway, but services that depend
      * on the ledger configuration (e.g., SubmissionService or CommandService) will reject all requests with the
      * UNAVAILABLE error.
      */
    configurationLoadTimeout: Duration,
)

object LedgerConfiguration {

  /** Default configuration for a ledger-backed index,
    * i.e., if there is zero delay between the ledger and the index.
    * Example: Sandbox classic.
    */
  val defaultLedgerBackedIndex: LedgerConfiguration = LedgerConfiguration(
    initialConfiguration = Configuration(
      generation = 1,
      timeModel = TimeModel.reasonableDefault,
      maxDeduplicationTime = Duration.ofDays(1)
    ),
    initialConfigurationSubmitDelay = Duration.ZERO,
    configurationLoadTimeout = Duration.ofSeconds(1),
  )

  /** Default configuration for a local single-participant ledger,
    * i.e., if there is minimal delay between the ledger and the index.
    * Example: Sandbox next.
    */
  val defaultLocalLedger: LedgerConfiguration = LedgerConfiguration(
    initialConfiguration = Configuration(
      generation = 1,
      timeModel = TimeModel.reasonableDefault,
      maxDeduplicationTime = Duration.ofDays(1)
    ),
    initialConfigurationSubmitDelay = Duration.ofMillis(500),
    configurationLoadTimeout = Duration.ofSeconds(5),
  )

  /** Default configuration for a participant connecting to a remote ledger,
    * i.e., if there may be significant delay between the ledger and the index.
    */
  val defaultRemote: LedgerConfiguration = LedgerConfiguration(
    initialConfiguration = Configuration(
      generation = 1,
      timeModel = TimeModel.reasonableDefault,
      maxDeduplicationTime = Duration.ofDays(1)
    ),
    initialConfigurationSubmitDelay = Duration.ofSeconds(5),
    configurationLoadTimeout = Duration.ofSeconds(10),
  )
}
