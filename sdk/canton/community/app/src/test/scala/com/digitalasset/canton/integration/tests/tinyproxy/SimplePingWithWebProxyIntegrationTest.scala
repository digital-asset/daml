// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.squid

import com.digitalasset.canton.integration.plugins.tinyproxy.UseTinyProxy
import com.digitalasset.canton.integration.plugins.tinyproxy.UseTinyProxy.TinyProxyConfig
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*

/** Simple test putting webproxy between a participant node and sequencer and testing that the
  * outgoing gRPC channels get correctly proxied through
  */
class SimplePingWithWebProxyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(_.focus(_.parameters.manualStart).replace(true))
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(
          Set(
            "participant1"
          )
        )
      )

  val tinyProxy = new UseTinyProxy(TinyProxyConfig("participant1"))
  registerPlugin(tinyProxy)

  "we can run a trivial ping" in { implicit env =>
    import env.*

    // Connect the participant to the sequencer via the proxy
    // The proxy pulls the participant out into an external process so we use remoteParticipant1 instead of participant1
    remoteParticipant1.synchronizers.connect(
      "da",
      s"http://localhost:${sequencer1.config.publicApi.port.unwrap}",
    )

    clue("maybe ping") {
      remoteParticipant1.health.maybe_ping(
        remoteParticipant1,
        timeout = 30.seconds,
      ) shouldBe defined
    }

    // Stop the proxy
    tinyProxy.stop()

    // Make sure ping fails
    remoteParticipant1.health.maybe_ping(
      remoteParticipant1,
      timeout = 5.seconds,
    ) shouldBe empty

    // Start it again
    tinyProxy.start()

    // Assert it works again
    clue("maybe ping") {
      remoteParticipant1.health.maybe_ping(
        remoteParticipant1,
        timeout = 30.seconds,
      ) shouldBe defined
    }
  }
}
