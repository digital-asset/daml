// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.signer

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.{PositiveDurationSeconds, SessionSigningKeysConfig}
import com.digitalasset.canton.crypto.{
  Signature,
  SignatureDelegationValidityPeriod,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.DefaultTestIdentities.participant1
import com.digitalasset.canton.topology.client.TopologySnapshot
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

class SyncCryptoSignerWithSessionKeysTest extends AnyWordSpec with SyncCryptoSignerTest {
  private lazy val validityDuration = config.PositiveDurationSeconds.ofSeconds(10)

  override protected lazy val sessionSigningKeysConfig: SessionSigningKeysConfig =
    SessionSigningKeysConfig.default
      .copy(keyValidityDuration = validityDuration)

  private def sessionKeysCache(p: SynchronizerCryptoClient) =
    p.syncCryptoSigner
      .asInstanceOf[SyncCryptoSignerWithSessionKeys]
      .sessionKeysSigningCache
      .asMap()

  private def cleanUpSessionKeysCache(p: SynchronizerCryptoClient): Unit =
    p.syncCryptoSigner
      .asInstanceOf[SyncCryptoSignerWithSessionKeys]
      .sessionKeysSigningCache
      .invalidateAll()

  private def cutOffValidityPercentage(p: SynchronizerCryptoClient) =
    p.syncCryptoSigner.asInstanceOf[SyncCryptoSignerWithSessionKeys].cutOffValidityPercentage

  private def setSessionKeyEvictionPeriod(
      p: SynchronizerCryptoClient,
      newPeriod: config.PositiveDurationSeconds,
  ): Unit =
    p.syncCryptoSigner
      .asInstanceOf[SyncCryptoSignerWithSessionKeys]
      .sessionKeyEvictionPeriod
      .set(newPeriod)

  /* Verify that a signature delegation is currently stored in the cache and contains the correct
   * information: (1) it is signed by a long-term key, (2) the enclosing signature is correctly listed
   * as being signed by a session key, and (3) the validity period is correct.
   */
  private def checkSignatureDelegation(
      topologySnapshot: TopologySnapshot,
      signature: Signature,
      p: SynchronizerCryptoClient = p1,
      periodLength: PositiveSeconds =
        PositiveSeconds.tryOfSeconds(validityDuration.underlying.toSeconds),
  ): Assertion = {

    val cache = sessionKeysCache(p)
    val (_, sessionKeyAndDelegation) = cache
      .find { case (_, skD) =>
        signature.signatureDelegation.contains(skD.signatureDelegation)
      }
      .valueOrFail("no signature delegation")

    topologySnapshot
      .signingKeys(p.member, defaultUsage)
      .futureValueUS
      .map(_.id) should contain(sessionKeyAndDelegation.signatureDelegation.signature.signedBy)

    val sessionKeyId = sessionKeyAndDelegation.signatureDelegation.sessionKey.id
    val validityPeriod = sessionKeyAndDelegation.signatureDelegation.validityPeriod

    // The signature contains the session key in the 'signedBy' field.
    signature.signedBy shouldBe sessionKeyId

    // Verify it has the correct validity period
    validityPeriod shouldBe
      SignatureDelegationValidityPeriod(
        topologySnapshot.timestamp,
        periodLength,
      )
  }

  "A SyncCryptoSigner with session keys" must {

    "use correct sync crypto signer with session keys" in {
      syncCryptoSignerP1 shouldBe a[SyncCryptoSignerWithSessionKeys]
    }

    "correctly produce a signature delegation when signing a single message" in {

      val signature = syncCryptoSignerP1
        .sign(
          testSnapshot,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      checkSignatureDelegation(testSnapshot, signature)

    }

    "use a new session signing key when the cut-off period has elapsed" in {

      val (_, currentSessionKey) = sessionKeysCache(p1).loneElement
      // select a timestamp that is after the cut-off period
      val cutOffTimestamp =
        currentSessionKey.signatureDelegation.validityPeriod
          .computeCutOffTimestamp(
            cutOffValidityPercentage(p1)
          )
          .valueOrFail("fail to compute the cut-off timestamp")

      val afterCutOffSnapshot =
        testingTopology.topologySnapshot(timestampOfSnapshot = cutOffTimestamp.addMicros(100))

      val signature = syncCryptoSignerP1
        .sign(
          afterCutOffSnapshot,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      checkSignatureDelegation(afterCutOffSnapshot, signature)

      // There must be a second key in the cache because we used a different session key for the latest sign call.
      // The previous key, although still valid, has exceeded its cutoff period.
      sessionKeysCache(p1).size shouldBe 2

    }

    "use a new session key if the long-term key is no longer active" in {

      val oldLongTermKeyId = testSnapshot
        .signingKeys(participant1.member, defaultUsage)
        .futureValueUS
        .loneElement
        .id

      testingTopology.getTopology().freshKeys.set(true)

      val newSnapshotWithFreshKeys = testingTopology.topologySnapshot()
      val newLongTermKeyId =
        newSnapshotWithFreshKeys
          .signingKeys(participant1.member, defaultUsage)
          .futureValueUS
          .loneElement
          .id

      newLongTermKeyId should not be oldLongTermKeyId

      val signature = syncCryptoSignerP1
        .sign(
          newSnapshotWithFreshKeys,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      checkSignatureDelegation(newSnapshotWithFreshKeys, signature)

      signature.signatureDelegation
        .valueOrFail("no signature delegation")
        .delegatingKeyId shouldBe newLongTermKeyId

    }

    "session signing key is removed from the cache after the eviction period" in {
      cleanUpSessionKeysCache(p1)

      val newEvictionPeriod = PositiveDurationSeconds.ofSeconds(5)

      setSessionKeyEvictionPeriod(p1, newEvictionPeriod)
      sessionKeysCache(p1) shouldBe empty

      val signature = syncCryptoSignerP1
        .sign(
          testSnapshot,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      checkSignatureDelegation(testSnapshot, signature)

      Threading.sleep(newEvictionPeriod.duration.toMillis + 100L)

      eventually() {
        sessionKeysCache(p1).toSeq shouldBe empty
      }
    }

    behave like syncCryptoSignerTest()
  }
}
