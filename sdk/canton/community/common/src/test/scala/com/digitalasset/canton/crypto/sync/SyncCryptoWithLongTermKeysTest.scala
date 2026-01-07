// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.sync

import com.digitalasset.canton.config.SessionSigningKeysConfig
import com.digitalasset.canton.crypto.signer.SyncCryptoSignerWithLongTermKeys
import com.digitalasset.canton.topology.DefaultTestIdentities.participant1
import com.digitalasset.canton.topology.TestingTopology
import com.digitalasset.canton.util.ResourceUtil
import org.scalatest.wordspec.AnyWordSpec

class SyncCryptoWithLongTermKeysTest extends AnyWordSpec with SyncCryptoTest {
  // we explicitly disable any use of session signing keys
  override protected lazy val sessionSigningKeysConfig: SessionSigningKeysConfig =
    SessionSigningKeysConfig.disabled

  "A SyncCrypto with long-term keys" must {

    behave like syncCryptoSignerTest()

    "use correct sync crypto with long term keys" in {
      syncCryptoSignerP1 shouldBe a[SyncCryptoSignerWithLongTermKeys]
    }

    /* This test checks whether a node that does not use session keys can verify a signature
     * sent by a node that uses session keys (with a signature delegation defined).
     */
    "correctly verify signature that contains a delegation for a session key" in {

      // enable session keys just for signing
      val testingTopologyWithSessionKeys =
        TestingTopology()
          .withSimpleParticipants(participant1)
          .withCryptoConfig(
            cryptoConfigWithSessionSigningKeysConfig(SessionSigningKeysConfig.default)
          )
          .build(crypto, loggerFactory)

      ResourceUtil.withResource(
        testingTopologyWithSessionKeys.forOwnerAndSynchronizer(participant1)
      ) { p1WithSessionKey =>
        val signature = p1WithSessionKey.syncCryptoSigner
          .sign(
            testingTopologyWithSessionKeys.topologySnapshot(),
            None,
            hash,
            defaultUsage,
          )
          .valueOrFail("sign failed")
          .futureValueUS

        signature.signatureDelegation should not be empty

        syncCryptoVerifierP1
          .verifySignature(
            testSnapshot,
            hash,
            participant1.member,
            signature,
            defaultUsage,
          )
          .valueOrFail("verification failed")
          .futureValueUS
      }
    }

  }

}
