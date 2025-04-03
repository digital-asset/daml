// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.crypto.{KeyPurpose, SigningKeyUsage}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality

trait KeyRotationWithMultipleSequencersIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with KeyManagementTestHelper {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S2M2
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      }

  "Two onboarded sequencers" should {
    "be able to rotate keys correctly" in { implicit env =>
      import env.*

      forAll(Seq(sequencer1, sequencer2)) {
        _.health.initialized() shouldBe true
      }

      rotateAndTest(sequencer1, environment.clock.now)
      rotateAndTest(sequencer2, environment.clock.now)
      loggerFactory.assertLogsUnorderedOptional(
        rotateAndTest(mediator1, environment.clock.now),
        (
          LogEntryOptionality.OptionalMany,
          /* It can occur, particularly when multiple sequencers are in use, that the first owner_to_key_mapping
           * update, which includes the addition of the new key, is still being processed by the sequencer when
           * the second owner_to_key_mapping update (that removes the old key) arrives. In such
           * instances, the sequencer is unable to verify the signature since it is still waiting for the processing
           * of the first update to be completed. To fix this, we make repeated attempts for this request until
           * it succeeds, and we ignore any errors that may arise.
           */
          logEntry => {
            logEntry.loggerName should include(
              "KeyRotationWithMultipleSequencers"
            )
            logEntry.errorMessage should (include(
              "Failed sending topology transaction broadcast: TopologyTransactionsBroadcast RequestRefused"
            ) or include("Failed broadcasting topology transactions: RequestRefused") or
              include("failed the following topology transactions"))
          },
        ),
      )

      participant1.health.ping(participant1)
    }

    "fail if we try to rotate a key that does not pertain to the correct sequencer" in {
      implicit env =>
        import env.*

        val owner = sequencer1.id.member
        val currentSigningKey =
          getCurrentKey(sequencer1, KeyPurpose.Signing, Some(SigningKeyUsage.ProtocolOnly))

        // Sequencer nodes only have signing keys
        val newKey = sequencer1.keys.secret
          .generate_signing_key(
            keySpec = Some(sequencer1.crypto.privateCrypto.defaultSigningKeySpec),
            usage = SigningKeyUsage.NamespaceOnly,
          )

        // trying to rotate the key for the node in the topology management using a different sequencer must fail
        assertThrows[IllegalArgumentException](
          sequencer2.topology.owner_to_key_mappings.rotate_key(
            owner,
            currentSigningKey,
            newKey,
          )
        )
    }
  }
}

class KeyRotationWithMultipleSequencersReferenceIntegrationTestPostgres
    extends KeyRotationWithMultipleSequencersIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory
    )
  )
}
