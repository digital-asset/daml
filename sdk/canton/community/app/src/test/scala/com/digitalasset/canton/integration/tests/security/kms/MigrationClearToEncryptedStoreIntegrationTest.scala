// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.Attack
import com.digitalasset.canton.integration.plugins.UseKms
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.util.ResourceUtil.withResource

trait MigrationClearToEncryptedStoreIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EncryptedCryptoPrivateStoreTestHelpers {

  protected val kmsPlugin: UseKms

  protected val protectedNodes: Set[String] = Set("participant1")

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*
      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
    }

  "after migrating to an encrypted private key store, keys remain encrypted and are the same as the initial keys" taggedAs
    securityAsset.setAttack(
      Attack(
        actor = "untrusted db admin",
        threat = "attempting to read private keys after a migration",
        mitigation = "all keys that the db admin has access are encrypted",
      )
    ) in { implicit env =>
      import env.*
      // private keys (in clear) before migrating to an encrypted private store
      val initialEnvKeys = protectedNodes.map { nodeName =>
        (nodeName, checkAndReturnClearKeys(nodeName))
      }.toMap

      /* Restart nodes and modify environment with a new config file.
         Note that this means all clear rows in the store will be encrypted.
       */
      stopAllNodesIgnoringSequencerClientWarnings(env.stopAll())
      registerPlugin(kmsPlugin)

      withResource(
        manualCreateEnvironmentWithPreviousState(
          env.actualConfig
        )
      ) { implicit newEnv =>
        import newEnv.*

        participant1.synchronizers.reconnect_all()
        participant2.synchronizers.reconnect_all()

        participant1.health.ping(participant2.id)

        forAll(protectedNodes) { nodeName =>
          val decryptedKeys =
            checkAndDecryptKeys(nodeName)(newEnv)
          // keys are the same as the ones before the migration
          decryptedKeys.toSet shouldBe initialEnvKeys(nodeName).toSet
        }
      }
    }
}
