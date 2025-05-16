// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.sync

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.SessionSigningKeysConfig
import com.digitalasset.canton.crypto.signer.SyncCryptoSignerWithSessionKeys
import com.digitalasset.canton.crypto.{
  Signature,
  SignatureDelegation,
  SignatureDelegationValidityPeriod,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.DefaultTestIdentities.participant1
import com.digitalasset.canton.topology.client.TopologySnapshot
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.FiniteDuration

class SyncCryptoWithSessionKeysTest extends AnyWordSpec with SyncCryptoTest {
  override protected lazy val sessionSigningKeysConfig: SessionSigningKeysConfig =
    SessionSigningKeysConfig.default

  private lazy val validityDuration = sessionSigningKeysConfig.keyValidityDuration

  private def sessionKeysCache(p: SynchronizerCryptoClient) =
    p.syncCryptoSigner
      .asInstanceOf[SyncCryptoSignerWithSessionKeys]
      .sessionKeysSigningCache
      .asMap()

  private def sessionKeysVerificationCache(p: SynchronizerCryptoClient) =
    p.syncCryptoVerifier.sessionKeysVerificationCache.asMap().map { case (id, (sD, _)) => (id, sD) }

  private def cleanUpSessionKeysCache(p: SynchronizerCryptoClient): Unit = {
    p.syncCryptoSigner
      .asInstanceOf[SyncCryptoSignerWithSessionKeys]
      .sessionKeysSigningCache
      .invalidateAll()
    p.syncCryptoSigner.asInstanceOf[SyncCryptoSignerWithSessionKeys].pendingRequests.clear()
  }

  private def cleanUpSessionKeysVerificationCache(p: SynchronizerCryptoClient): Unit =
    p.syncCryptoVerifier.sessionKeysVerificationCache
      .invalidateAll()

  private def cutOffDuration(p: SynchronizerCryptoClient) =
    p.syncCryptoSigner.asInstanceOf[SyncCryptoSignerWithSessionKeys].cutOffDuration

  private def setSessionKeyEvictionPeriod(
      p: SynchronizerCryptoClient,
      newPeriod: FiniteDuration,
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
      validityPeriodLength: PositiveSeconds =
        PositiveSeconds.tryOfSeconds(validityDuration.underlying.toSeconds),
  ): SignatureDelegation = {

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
    val margin = cutOffDuration(p).unwrap.dividedBy(2)
    validityPeriod shouldBe
      SignatureDelegationValidityPeriod(
        topologySnapshot.timestamp.minus(margin),
        validityPeriodLength,
      )

    sessionKeyAndDelegation.signatureDelegation
  }

  private def cleanCache(p: SynchronizerCryptoClient) = {
    // make sure we start from a clean state
    cleanUpSessionKeysCache(p)
    cleanUpSessionKeysVerificationCache(p)

    sessionKeysCache(p) shouldBe empty
    sessionKeysVerificationCache(p) shouldBe empty
  }

  "A SyncCrypto with session keys" must {

    behave like syncCryptoSignerTest()

    "use correct sync crypto signer with session keys" in {
      syncCryptoSignerP1 shouldBe a[SyncCryptoSignerWithSessionKeys]
      cleanCache(p1)
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

      val signatureDelegation = checkSignatureDelegation(testSnapshot, signature)

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

      val (_, sDSigningCached) = sessionKeysCache(p1).loneElement
      val (_, sDVerificationCached) = sessionKeysVerificationCache(p1).loneElement

      // make sure that nothing changed with the session key and signature delegation
      sDSigningCached.signatureDelegation shouldBe signatureDelegation
      sDSigningCached.signatureDelegation shouldBe sDVerificationCached

    }

    "sign and verify message with different synchronizers uses different session keys" in {
      val p1OtherSynchronizer =
        testingTopology.forOwnerAndSynchronizer(
          owner = participant1,
          synchronizerId = otherSynchronizerId,
        )
      val syncCryptoSignerP1Other = p1OtherSynchronizer.syncCryptoSigner

      val signature = syncCryptoSignerP1Other
        .sign(
          testSnapshot,
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS

      val signatureDelegationOther =
        checkSignatureDelegation(testSnapshot, signature, p1OtherSynchronizer)

      sessionKeysCache(p1OtherSynchronizer).loneElement

      // it's different from the signature delegation for the other synchronizer
      val (_, signatureDelegation) = sessionKeysCache(p1).loneElement
      signatureDelegationOther.validityPeriod shouldBe signatureDelegation.signatureDelegation.validityPeriod
      signatureDelegationOther.sessionKey should not be signatureDelegation.signatureDelegation.sessionKey
      signatureDelegationOther.signature should not be signatureDelegation.signatureDelegation.signature
    }

    "use a new session signing key when the cut-off period has elapsed" in {

      val (_, currentSessionKey) = sessionKeysCache(p1).loneElement
      // select a timestamp that is after the cut-off period
      val cutOffTimestamp =
        currentSessionKey.signatureDelegation.validityPeriod
          .computeCutOffTimestamp(cutOffDuration(p1))

      val afterCutOffSnapshot =
        testingTopology.topologySnapshot(timestampOfSnapshot = cutOffTimestamp)

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

      val newEvictionPeriod = PositiveSeconds.tryOfSeconds(5).toFiniteDuration

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

      Threading.sleep(newEvictionPeriod.toMillis + 100L)

      eventually() {
        sessionKeysCache(p1).toSeq shouldBe empty
      }
    }

    "with decreasing timestamps we still use one session signing key" in {

      cleanCache(p1)
      testingTopology.getTopology().freshKeys.set(false)

      /* This covers a test scenario where we receive a decreasing timestamp order of signing requests
          and verifies that since the validity period of a key is set as ts-cutOff/2 to ts+x+cutOff/2 we
          can still use the same session key created for the first request.
       */

      val signatureDelegation3 = syncCryptoSignerP1
        .sign(
          testingTopology.topologySnapshot(timestampOfSnapshot = CantonTimestamp.ofEpochSecond(3)),
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS
        .signatureDelegation
        .valueOrFail("no signature delegation")

      val signatureDelegation2 = syncCryptoSignerP1
        .sign(
          testingTopology.topologySnapshot(timestampOfSnapshot = CantonTimestamp.ofEpochSecond(2)),
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS
        .signatureDelegation
        .valueOrFail("no signature delegation")

      val signatureDelegation1 = syncCryptoSignerP1
        .sign(
          testingTopology.topologySnapshot(timestampOfSnapshot = CantonTimestamp.ofEpochSecond(1)),
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS
        .signatureDelegation
        .valueOrFail("no signature delegation")

      signatureDelegation1 should equal(signatureDelegation2)
      signatureDelegation2 should equal(signatureDelegation3)

      // enough time has elapsed and the session key is no longer valid

      val signatureDelegationNew = syncCryptoSignerP1
        .sign(
          testingTopology.topologySnapshot(timestampOfSnapshot =
            CantonTimestamp.Epoch
              .addMicros(validityDuration.unwrap.toMicros)
              .add(cutOffDuration(p1).duration)
          ),
          hash,
          defaultUsage,
        )
        .valueOrFail("sign failed")
        .futureValueUS
        .signatureDelegation
        .valueOrFail("no signature delegation")

      signatureDelegationNew should not equal (signatureDelegation1)

    }

  }
}
