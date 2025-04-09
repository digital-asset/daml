// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.connection

import com.digitalasset.canton.config
import com.digitalasset.canton.config.StorageConfig
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*

/** Trivial test which can be used as a first end to end test */
trait ApiInfoIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      // Make synchronizer connection fail faster
      .addConfigTransform(
        ConfigTransforms.updateAllSequencerClientConfigs_(
          _.focus(_.maxConnectionRetryDelay).replace(config.NonNegativeFiniteDuration.ofSeconds(1))
        )
      )
      .addConfigTransform(x =>
        x.focus(_.parameters.timeouts.processing.sequencerInfo)
          .replace(config.NonNegativeDuration.tryFromDuration(2.seconds))
      )

  "port misconfiguration" should {
    "raise meaningful error message" in { implicit env =>
      import env.*

      val adminPort = sequencer1.config.adminApi.port
      assertThrowsAndLogsCommandFailures(
        participant1.synchronizers
          .connect(daName, s"http://localhost:$adminPort", manualConnect = true),
        _.errorMessage should (
          include(s"localhost:$adminPort") and
            include(
              s"provides '${CantonGrpcUtil.ApiName.AdminApi}', expected '${CantonGrpcUtil.ApiName.SequencerPublicApi}'"
            ) and
            include("This message indicates a possible mistake in configuration") and
            include(s"please check node connection settings for '${daName.unwrap}'")
        ),
      )
    }
  }
}

class ApiInfoIntegrationTestInMemory extends ApiInfoIntegrationTest {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .addConfigTransform(ConfigTransforms.allInMemory)
      .addConfigTransform(_.focus(_.monitoring.logging.api.messagePayloads).replace(false))

  registerPlugin(new UseCommunityReferenceBlockSequencer[StorageConfig.Memory](loggerFactory))

}
