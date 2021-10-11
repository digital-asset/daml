// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.time.Duration

import com.daml.ledger.configuration.Configuration

/** Instructions on how to initialize an empty ledger, without a configuration.
  *
  * A configuation is only submitted if one is not detected on the ledger.
  *
  * @param configuration         The initial ledger configuration to submit.
  * @param delayBeforeSubmitting The delay until the participant tries to submit a configuration.
  */
final case class InitialLedgerConfiguration(
    configuration: Configuration,
    delayBeforeSubmitting: Duration,
)
