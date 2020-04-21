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
      * The delay until the ledger API server tries to submit an initial configuration if none exists.
      */
    initialConfigurationSubmitDelay: Duration,
)

object LedgerConfiguration {
  val default: LedgerConfiguration = LedgerConfiguration(
    initialConfiguration = Configuration(
      generation = 0,
      timeModel = TimeModel.reasonableDefault,
      maxDeduplicationTime = Duration.ofDays(1)
    ),
    initialConfigurationSubmitDelay = Duration.ZERO
  )
}
