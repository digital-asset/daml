// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security.kms

import cats.syntax.parallel.*
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.Attack
import com.digitalasset.canton.config.KmsConfig
import com.digitalasset.canton.crypto.kms.mock.v1.MockKmsDriverFactory.mockKmsDriverName
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError.FailedToReadKey
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}

/** Defines the different environments and tests to be used by the EncryptedCryptoPrivateStore...
  * integration tests (i.e. ...WithPreDefinedKey...; ...NoPreDefinedKey...;
  * ...NoPreDefinedKeyMultiRegion...)
  */
trait EncryptedCryptoPrivateStoreIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EncryptedCryptoPrivateStoreTestHelpers {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*
      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
    }

  protected val protectedNodes: Set[String] = Set("participant1")

  "participants can ping each other" in { implicit env =>
    import env.*
    participant1.health.ping(participant2.id)
  }

  "participants can ping each other after restart" in { implicit env =>
    import env.*

    // TODO(#25069): Add persistence to mock KMS driver to support node restarts as used in this test case.
    //               Cancel this test case when mock kms driver is used.
    assume(
      participant1.config.crypto.kms.exists(kms =>
        !(kms.isInstanceOf[KmsConfig.Driver]
          && kms.asInstanceOf[KmsConfig.Driver].name == mockKmsDriverName)
      )
    )

    /* Restart node 1 and verify that it can still ping.
     Note that a new KMS key has been created and restarting the node means fetching this newly created key
     'wrapper_key_id' from the database
     */
    participant1.stop()
    participant1.start()
    participant1.synchronizers.reconnect_all()

    participant1.health.ping(participant2.id)
  }

  "protected nodes have their stored private keys encrypted and these can be decrypted" taggedAs
    securityAsset.setAttack(
      Attack(
        actor = "malicious db admin",
        threat = "attempts to read private keys",
        mitigation = "encrypt all private keys in the database.",
      )
    ) in { implicit env =>
      forAll(protectedNodes) { nodeName =>
        checkAndDecryptKeys(nodeName)
      }
    }

  "decryption fails when a protected node has a non-encrypted key in an encrypted store" in {
    implicit env =>
      import env.*
      forAll(protectedNodes) { nodeName =>
        // store a clear key
        storeClearKey(nodeName)
        // TODO(i10760): remove encrypted flag from listPrivateKeys so that we can simply
        // call checkAndDecryptKeys and expect it to fail with a decrypt error
        // then this check should fail with a decryption error
        // checkAndDecryptKeys(nodeName, kms)
        val encStore = getEncryptedCryptoStore(nodeName)
        listAllStoredKeys(encStore.store).toList
          .parTraverse(storedKey => encStore.exportPrivateKey(storedKey.id))
          .leftOrFailShutdown("check encryption")
          .futureValue shouldBe a[FailedToReadKey]
      }
  }

}
