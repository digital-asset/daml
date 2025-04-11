// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.topology.UniqueIdentifier
import monocle.Monocle.toAppliedFocusOps
import org.slf4j.event.Level

import scala.util.Try

/** Test the has_identity console command. Also highlights that there is a possible window during
  * which the node is "running" but its node identity is not ready. To do this we start an in
  * process node with auto initialization disabled and set the node's id manually.
  */
class IdentityConsoleCommandsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment { self =>

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S1M1_Manual
      .addConfigTransform(
        ConfigTransforms.disableAutoInit(Set("sequencer1", "participant1", "mediator1"))
      )
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(
          Set(
            "participant1" // never completely initialized / not used
          )
        )
      )
  "has_identity returns the correct response" in { implicit env =>
    import env.*

    Seq[LocalInstanceReference](sequencer1, mediator1, participant1).foreach { node =>
      loggerFactory.assertLogsSeq(
        (SuppressionRule.LoggerNameContains("SetupNodeId") && SuppressionRule.Level(Level.INFO))
      )(
        {
          // Start will run until external input is required
          node.start()
          node.health.wait_for_running()
          node.health.has_identity() shouldBe false
        },
        entries =>
          // This assertion confirms that the node is indeed started with auto initialization disabled
          forAtLeast(1, entries.toList) {
            _.message should include(
              "Startup succeeded up to stage Init node id, waiting for external input"
            )
          },
      )

      val key =
        node.keys.secret.generate_signing_key("namespace", usage = SigningKeyUsage.NamespaceOnly)
      val uid = UniqueIdentifier.tryCreate(node.name, key.fingerprint)
      eventually() {
        Try(node.topology.init_id(uid)).success
      }
      // init_id should only return after the next bootstrap stage has been reached
      // and therefore the identity should be properly set
      node.health.wait_for_identity()
      node.health.has_identity() shouldBe true
    }
  }
}
