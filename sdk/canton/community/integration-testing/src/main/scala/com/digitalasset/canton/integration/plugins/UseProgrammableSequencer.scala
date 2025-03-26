// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencer

class UseProgrammableSequencer(
    environmentId: String,
    override protected val loggerFactory: NamedLoggerFactory,
) extends EnvironmentSetupPlugin {
  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    ProgrammableSequencer.configOverride(environmentId, loggerFactory)(config)
}
