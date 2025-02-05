// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.config.{DefaultProcessingTimeouts, SessionSigningKeysConfig}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.signer.{
  SyncCryptoSignerDefault,
  SyncCryptoSignerWithSessionKeys,
}
import com.digitalasset.canton.topology.DefaultTestIdentities.participant1
import com.digitalasset.canton.topology.TestingTopology
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class SyncCryptoSignerTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  private lazy val crypto = SymbolicCrypto.create(
    testedReleaseProtocolVersion,
    DefaultProcessingTimeouts.testing,
    loggerFactory,
  )

  // TODO(#23732): Generalize tests to work for both SyncCryptoSigner(s), with and without session signing keys
  "use correct sync crypto signer with session keys" in {
    def createTestingTopology(sessionSigningKeysConfig: SessionSigningKeysConfig) =
      TestingTopology
        .from(
          participants = Seq(
            participant1
          ).map((_, ParticipantAttributes(ParticipantPermission.Confirmation))).toMap,
          sessionSigningKeysConfig = sessionSigningKeysConfig,
        )
        .build(crypto, loggerFactory)

    // enable session signing keys in the config
    val testTopologyWithSessionSigningKeys = createTestingTopology(SessionSigningKeysConfig.default)
    // create the signer based on the config, match that it is with session keys enabled
    val p1WithSessionSigningKeys =
      testTopologyWithSessionSigningKeys.forOwnerAndSynchronizer(participant1)
    p1WithSessionSigningKeys.syncCryptoSigner shouldBe a[SyncCryptoSignerWithSessionKeys]

    // disable session signing keys in the config
    val testingTopologyDefault = createTestingTopology(SessionSigningKeysConfig.disabled)
    // create the signer based on the config, match that it does not run with session keys
    val p1Default = testingTopologyDefault.forOwnerAndSynchronizer(participant1)
    p1Default.syncCryptoSigner shouldBe a[SyncCryptoSignerDefault]
  }

}
