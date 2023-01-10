// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.runner.common.ConfigAdaptor
import com.daml.platform.configuration.InitialLedgerConfiguration

import java.time.Duration

class BridgeConfigAdaptor extends ConfigAdaptor {
  override def initialLedgerConfig(
      maxDeduplicationDuration: Option[Duration]
  ): InitialLedgerConfiguration = {
    val superConfig = super.initialLedgerConfig(maxDeduplicationDuration)
    superConfig.copy(maxDeduplicationDuration = BridgeConfig.DefaultMaximumDeduplicationDuration)
  }
}
