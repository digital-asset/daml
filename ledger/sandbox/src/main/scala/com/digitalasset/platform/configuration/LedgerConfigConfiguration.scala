// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.time.Duration

import com.daml.ledger.participant.state.v1.{Configuration, TimeModel}

/**
  * Configuration surrounding parties and party allocation.
  */
case class LedgerConfigConfiguration(
    /**
      * The default configuration to use if the ledger does not contain one yet.
      */
    defaultConfiguration: Configuration,
    /**
      * The delay until the ledger API server tries to write an initial configuration if none exists.
      */
    initialConfigSubmitDelay: Duration,
)

object LedgerConfigConfiguration {
  val default: LedgerConfigConfiguration = LedgerConfigConfiguration(
    defaultConfiguration = Configuration(
      generation = 0,
      timeModel = TimeModel.reasonableDefault,
      maxDeduplicationTime = Duration.ofDays(1)
    ),
    initialConfigSubmitDelay = Duration.ZERO
  )
}
