// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig

/** Configure the behaviour of the Daml Engine
  *
  * @param enableEngineStackTraces If true, DAMLe stack traces will be enabled
  * @param iterationsBetweenInterruptions Number of engine iterations between forced interruptions (outside needs of information).
  * @param submissionPhaseLogging Configuration for logging in phase one (command submission) of canton transaction processing
  * @param validationPhaseLogging Configuration for logging in phase three (transaction validation) of canton transaction processing
  */
final case class CantonEngineConfig(
    enableEngineStackTraces: Boolean = false,
    iterationsBetweenInterruptions: Long =
      10000, // 10000 is the default value in the engine configuration
    submissionPhaseLogging: EngineLoggingConfig = EngineLoggingConfig(enabled = true),
    validationPhaseLogging: EngineLoggingConfig = EngineLoggingConfig(enabled = false),
)
