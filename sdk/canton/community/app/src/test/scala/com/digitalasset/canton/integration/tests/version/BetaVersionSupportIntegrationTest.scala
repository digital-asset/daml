// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.version

import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.ConfigTransforms.setNonStandardConfig
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError.HandshakeErrors.HandshakeFailed
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

trait BetaVersionSupportIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with FlagCloseable
    with HasCloseContext {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1_Manual
      .addConfigTransforms(ConfigTransforms.setBetaSupport(true)*)
      .addConfigTransforms(ConfigTransforms.setAlphaVersionSupport(false)*)
      .addConfigTransforms(
        setNonStandardConfig(true),
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.parameters.betaVersionSupport).replace(false)
        ),
        ConfigTransforms.updateParticipantConfig("participant3") { config =>
          config
            .focus(_.parameters.alphaVersionSupport)
            .replace(true) // enable dev version support
            .focus(_.parameters.betaVersionSupport)
            .replace(false)

        },
        ConfigTransforms.updateSequencerConfig("sequencer2")(
          _.focus(_.parameters.betaVersionSupport).replace(false)
        ),
      )
      .withManualStart

  "Starting synchronizers with a beta protocol version" should {
    "work for synchronizer where beta version support is enabled" onlyRunWhen (_.isBeta) in {
      implicit env =>
        import env.*

        nodes.local.start()
        eventually() {
          sequencer1.health.status.successOption should not be empty
        }
        environment.config.parameters.betaVersionSupport shouldBe true
        sequencer1.synchronizer_parameters.static
          .get()
          .protocolVersion shouldBe testedProtocolVersion

        clue("connecting participant1 which has beta version support enabled") {
          participant1.synchronizers.connect(sequencer1, daName)
        }
        clue(
          "connecting participant3 which has dev version support enabled but not beta version support"
        ) {
          participant3.synchronizers.connect(sequencer1, daName)
        }
        participant1.health.ping(participant1.id)
    }

    "not be able to connect to a synchronizer where betaVersionSupport is disabled" onlyRunWhen (_.isBeta) in {
      env =>
        import env.*
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant2.synchronizers.connect(sequencer1, daName),
          _.shouldBeCantonErrorCode(HandshakeFailed),
        )
    }

    "fail starting synchronizer where betaVersionSupport is disabled" onlyRunWhen (_.isBeta) in {
      implicit env =>
        import env.*
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          sequencer2.start(),
          _.message should include("Sequencer handshake failed"),
          _.message should include(
            s"The protocol version required by the server (${ProtocolVersion.beta.map(_.v).mkString(", ")}) is not among the supported protocol versions by the client Vector(${ProtocolVersion.stable.map(_.v).mkString(", ")}"
          ),
        )
    }
  }

}

//class BetaVersionSupportIntegrationTestH2 extends BetaVersionSupportIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//}

class BetaVersionSupportIntegrationTestPostgres extends BetaVersionSupportIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
