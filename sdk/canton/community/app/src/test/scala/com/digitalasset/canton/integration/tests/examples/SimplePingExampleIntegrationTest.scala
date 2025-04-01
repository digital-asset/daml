// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.CommunityIntegrationTest
import com.digitalasset.canton.integration.plugins.{UseCommunityReferenceBlockSequencer, UseH2}
import com.digitalasset.canton.integration.tests.examples.ExampleIntegrationTest.{
  ensureSystemProperties,
  simpleTopology,
}

sealed abstract class SimplePingExampleIntegrationTest
    extends ExampleIntegrationTest(simpleTopology / "simple-topology.conf")
    with CommunityIntegrationTest {

  "run simple-ping.canton successfully" in { implicit env =>
    import env.*
    val port = sequencer1.sequencerConnection.endpoints.head.port.unwrap.toString
    ensureSystemProperties(("canton-examples.da-port", port))
    runScript(simpleTopology / "simple-ping.canton")(environment)
  }
}

final class SimplePingExampleReferenceIntegrationTestH2 extends SimplePingExampleIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}
