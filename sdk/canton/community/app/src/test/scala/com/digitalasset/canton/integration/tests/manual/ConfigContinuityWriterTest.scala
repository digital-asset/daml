// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.integration.tests.manual.DataContinuityTest.releaseVersion
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}

import java.nio.file.Files

final class ConfigContinuityWriterTest extends CommunityIntegrationTest with SharedEnvironment {
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1

  // On the first shard, also dump the config
  "Data continuity for config" should {
    s"correctly generate default config dump for (release=$releaseVersion)" in { env =>
      val config = env.actualConfig.copy(
        mediators = env.actualConfig.mediators.take(1),
        participants = env.actualConfig.participants.take(1),
        sequencers = env.actualConfig.sequencers.take(1),
      )
      Files.createDirectories(DataContinuityTest.baseDumpForRelease.path)
      CantonConfig.save(
        config,
        (DataContinuityTest.baseDumpForRelease.directory / "default.conf").pathAsString,
      )
    }
  }
}
