// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import com.digitalasset.canton.integration.plugins.*
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.util.ResourceUtil.withResource

trait MigrationEncryptedToClearStoreIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EncryptedCryptoPrivateStoreTestHelpers {

  protected val kmsRevertPlugin: UseKms

  protected val protectedNodes: Set[String] = Set("participant1")

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*
      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
    }

  "after revoking an encrypted private key store, keys are in clear and are the same as the initial encrypted keys" in {
    implicit env =>
      // private keys (encrypted) before migrating to a clear private store
      val initialEnvKeys = protectedNodes.map { nodeName =>
        (nodeName, checkAndDecryptKeys(nodeName))
      }.toMap

      /* Restart nodes and modify environment with a new config file with the encrypted store set to be revoked.
       Note that this means that canton will decrypt the keys and use a non-encrypted store.
       */
      registerPlugin(kmsRevertPlugin)
      stopAllNodesIgnoringSequencerClientWarnings(env.stopAll())

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
          val currentKeys = checkAndReturnClearKeys(nodeName)(newEnv.executionContext, newEnv)
          // keys are the same as the ones before the migration
          currentKeys.toSet shouldBe initialEnvKeys(nodeName).toSet
        }
      }
  }
}
