// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreExtended, CryptoPublicStore}
import com.digitalasset.canton.crypto.{
  CryptoKeyPairKey,
  Fingerprint,
  KeyPurpose,
  SigningPrivateKey,
  SigningPublicKey,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.security.kms.EncryptedCryptoPrivateStoreTestHelpers
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import org.scalatest.Assertion

/** Crypto keys that are in a legacy format are migrated upon deserialization. This can happen for
  * various situations, such as reading keys from the crypto stores or importing a key via a gRPC
  * service. As we don't want the keys in a legacy format to be kept around longer than necessary,
  * the crypto stores read all the keys during initialization, thereby triggering a possible
  * migration, and rewrite back the ones that were actually migrated.
  *
  * This test initializes a synchronizer and forces a conversion of the keys in the crypto stores to
  * a legacy format. It then stops and restarts the nodes, triggering the mechanism described above.
  * After this step, the test reads the keys and checks that they are in the new format and did not
  * undergo a migration.
  */
trait KeyFormatMigrationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EncryptedCryptoPrivateStoreTestHelpers {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*
      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
    }

  "crypto store initializations perform key migration" in { implicit env =>
    import env.*

    val testedNodes =
      Seq[LocalInstanceReference](participant1, participant2, sequencer1, mediator1)

    // Read the keys from the public store and reverse-migrate the legacy keys
    val initialPublicKeys = testedNodes.map { node =>
      val publicStore = getPublicStore(node)
      val publicKeys = getInitialKeysAndSetupLegacy[SigningPublicKey](
        getKeys = () => getPublicKeys(publicStore),
        setupLegacy =
          () => publicStore.reverseMigration().futureValueUS.valueOrFail("reverse-migrate"),
        compareKeys = (k1, k2) => {
          k1.keySpec shouldBe k2.keySpec
          k1.format shouldBe k2.format
        },
      )

      node -> publicKeys
    }.toMap
    initialPublicKeys should not be empty

    // Read the keys from the private store and reverse-migrate the legacy keys
    val initialPrivateKeys = testedNodes.map { node =>
      val privateStore = getPrivateStore(node)
      val privateKeys = getInitialKeysAndSetupLegacy[SigningPrivateKey](
        getKeys = () => getPrivateKeys(privateStore),
        setupLegacy =
          () => privateStore.reverseMigration().futureValueUS.valueOrFail("reverse-migrate"),
        compareKeys = (k1, k2) => {
          k1.keySpec shouldBe k2.keySpec
          k1.format shouldBe k2.format
        },
      )

      node -> privateKeys
    }.toMap
    initialPrivateKeys should not be empty

    // Restart nodes to trigger a key migration
    stopAllNodesIgnoringSequencerClientWarnings(env.stopAll())
    env.startAll()

    participant1.synchronizers.reconnect_all()
    participant2.synchronizers.reconnect_all()

    participant1.health.ping(participant2.id)

    val currentPublicKeys = testedNodes.map { node =>
      val publicStore = getPublicStore(node)
      val currentKeys = getPublicKeys(publicStore)
      // Keys have been migrated at startup, so they don't show as migrated when read
      forAll(currentKeys)(_.migrated shouldBe false)

      node -> currentKeys
    }.toMap

    val currentPrivateKeys = testedNodes.map { node =>
      val privateStore = getPrivateStore(node)
      val currentKeys = getPrivateKeys(privateStore)
      // Keys have been migrated at startup, so they don't show as migrated when read
      forAll(currentKeys)(_.migrated shouldBe false)

      node -> currentKeys
    }.toMap

    // Keys are the same as the initial ones
    currentPublicKeys shouldBe initialPublicKeys
    currentPrivateKeys shouldBe initialPrivateKeys
  }

  private def getPublicKeys(store: CryptoPublicStore): Set[SigningPublicKey] =
    store.signingKeys(traceContext).futureValueUS

  private def getPrivateKeys(store: CryptoPrivateStoreExtended): Set[SigningPrivateKey] =
    store
      .listPrivateKeys(KeyPurpose.Signing)
      .futureValueUS
      .valueOrFail("list private keys")
      .map(storedKey =>
        SigningPrivateKey.fromTrustedByteString(storedKey.data).valueOrFail("deserialize")
      )

  private def getPublicStore(nodeName: LocalInstanceReference): CryptoPublicStore =
    nodeName.crypto.cryptoPublicStore

  private def getPrivateStore(nodeName: LocalInstanceReference): CryptoPrivateStoreExtended =
    nodeName.crypto.cryptoPrivateStore.toExtended.valueOrFail("extend private store")

  private def getInitialKeysAndSetupLegacy[K <: CryptoKeyPairKey](
      getKeys: () => Set[K],
      setupLegacy: () => Seq[Fingerprint],
      compareKeys: (K, K) => Assertion,
  ): Set[K] = {
    val keys = getKeys()
    forAll(keys)(_.migrated shouldBe false)

    val migratedKeyIds = setupLegacy()
    val migratedKeys = getKeys().filter(k => migratedKeyIds.contains(k.id))
    migratedKeys should not be empty

    forAll(migratedKeys) { migratedKey =>
      val originalKey = keys.find(_.id == migratedKey.id).valueOrFail("find key")
      migratedKey.migrated shouldBe true
      compareKeys(migratedKey, originalKey)
    }

    keys
  }
}

class KeyFormatMigrationIntegrationTestPostgres extends KeyFormatMigrationIntegrationTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UsePostgres(loggerFactory))
}
