// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.version

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.{UseCommunityReferenceBlockSequencer, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.version.{ParticipantProtocolVersion, TestProtocolVersions}
import monocle.macros.syntax.lens.*

/* Separate test as different configuration is needed */
class FailingMinimumHandshakeIntegrationTestH2
    extends CommunityIntegrationTest
    with SharedEnvironment {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private lazy val participantVersion = Some(
    ParticipantProtocolVersion(TestProtocolVersions.UnreleasedValidPV)
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.allInMemory,
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.minimumProtocolVersion).replace(participantVersion)
        ),
      )

  s"Handshake fails when ${TestProtocolVersions.UnreleasedValidPV} is the minimum protocol version" in {
    implicit env: TestConsoleEnvironment =>
      import env.*

      def connectP1(): Unit = participant1.synchronizers.connect_local(sequencer1, daName)

      if (testedProtocolVersion.isStable) {
        loggerFactory.assertLogs(
          a[CommandFailure] should be thrownBy connectP1(),
          _.errorMessage should (include("HandshakeFailedError") and include(
            s"minimum version configured by the participant (${TestProtocolVersions.UnreleasedValidPV})"
          )),
        )
      } else connectP1()
  }
}
