// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.sync

import com.digitalasset.canton.config.SessionSigningKeysConfig
import com.digitalasset.canton.crypto.signer.SyncCryptoSignerWithLongTermKeys
import com.digitalasset.canton.topology.DefaultTestIdentities.participant1
import com.digitalasset.canton.topology.TestingTopology
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

    "correctly verify signature that contains a delegation for a session key" in {

      val testingTopologyWithSessionKeys =
        TestingTopology(sessionSigningKeysConfig = SessionSigningKeysConfig.default)
          .withSimpleParticipants(participant1)
          .build(crypto, loggerFactory)

      val p1WithSessionKey = testingTopologyWithSessionKeys.forOwnerAndSynchronizer(participant1)

      val signature = p1WithSessionKey.syncCryptoSigner
        .sign(
          testingTopologyWithSessionKeys.topologySnapshot(),
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
