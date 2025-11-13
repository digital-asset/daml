// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import com.digitalasset.canton.config.{CantonConfig, CommunityCantonEdition, TestingConfigInternal}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNodeBootstrapFactoryImpl
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeBootstrapFactoryImpl
import com.digitalasset.canton.synchronizer.sequencer.SequencerNodeBootstrapFactoryImpl

object CommunityEnvironmentFactory extends EnvironmentFactory {

  override def create(
      config: CantonConfig,
      loggerFactory: NamedLoggerFactory,
      testingConfigInternal: TestingConfigInternal,
  ): Environment =
    new Environment(
      config,
      CommunityCantonEdition,
      testingConfigInternal,
      ParticipantNodeBootstrapFactoryImpl,
      SequencerNodeBootstrapFactoryImpl,
      MediatorNodeBootstrapFactoryImpl,
      loggerFactory,
    )
}
