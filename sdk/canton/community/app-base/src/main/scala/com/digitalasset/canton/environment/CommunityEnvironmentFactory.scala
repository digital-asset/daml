// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import com.digitalasset.canton.config.{CantonConfig, CommunityCantonEdition, TestingConfigInternal}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.CommunityParticipantNodeBootstrapFactory
import com.digitalasset.canton.resource.CommunityDbMigrationsMetaFactory
import com.digitalasset.canton.synchronizer.mediator.CommunityMediatorNodeBootstrapFactory
import com.digitalasset.canton.synchronizer.sequencer.CommunitySequencerNodeBootstrapFactory

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
      CommunityParticipantNodeBootstrapFactory,
      CommunitySequencerNodeBootstrapFactory,
      CommunityMediatorNodeBootstrapFactory,
      new CommunityDbMigrationsMetaFactory(loggerFactory),
      loggerFactory,
    )
}
