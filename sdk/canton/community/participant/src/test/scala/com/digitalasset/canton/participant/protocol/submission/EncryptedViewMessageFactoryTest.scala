// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{
  AsymmetricEncrypted,
  CryptoPureApi,
  Fingerprint,
  SessionKeyAndReference,
  SessionKeyInfo,
}
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.{
  RandomnessAndReference,
  RandomnessRevocationInfo,
  ViewParticipantsKeysAndParentRecipients,
}
import com.digitalasset.canton.sequencing.protocol.{
  MemberRecipient,
  Recipient,
  Recipients,
  RecipientsTree,
}
import com.digitalasset.canton.store.SessionKeyStore.RecipientGroup
import com.digitalasset.canton.topology.ParticipantId
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

class EncryptedViewMessageFactoryTest extends AsyncWordSpec with BaseTest {

  private lazy val pureCrypto: CryptoPureApi = new SymbolicPureCrypto()

  private lazy val alice: ParticipantId = ParticipantId("Alice")
  private lazy val keyFingerprintAlice: Fingerprint = Fingerprint.tryCreate("alice_encryptionKey")

  private lazy val bob: ParticipantId = ParticipantId("Bob")
  private lazy val charlie: ParticipantId = ParticipantId("Charlie")

  private def createLeafRecipientsTreeWithSingleMember(member: ParticipantId) =
    Recipients.cc(MemberRecipient(member)).trees

  private def createRecipientsTreeWithSingleMember(
      member: ParticipantId,
      children: NonEmpty[Seq[RecipientsTree]],
  ) =
    NonEmpty(
      Seq,
      RecipientsTree(
        NonEmpty.mk(Set, MemberRecipient(member): Recipient),
        children.forgetNE,
      ),
    )

  /* A simple transaction with 3 views/nodes with a different recipient for each one.
   *
   *               n0:RT[alice]
   *              /            \
   *  n1:(RT[alice], RT[bob])   n2:(RT[alice], RT[charlie])
   */

  // The recipients for n1 where Bob is the leaf recipient
  private lazy val recipientsGroupBob =
    RecipientGroup(
      Recipients(
        createRecipientsTreeWithSingleMember(
          alice,
          createLeafRecipientsTreeWithSingleMember(bob),
        )
      ),
      pureCrypto.defaultSymmetricKeyScheme,
    )

  // The recipients for n2 where Charlie is the leaf recipient
  private lazy val recipientsGroupCharlie =
    RecipientGroup(
      Recipients(
        createRecipientsTreeWithSingleMember(
          alice,
          createLeafRecipientsTreeWithSingleMember(charlie),
        )
      ),
      pureCrypto.defaultSymmetricKeyScheme,
    )

  // The recipients for the root node n0 where Alice is the only recipient
  private lazy val recipientsGroupAlice =
    RecipientGroup(Recipients.cc(alice), pureCrypto.defaultSymmetricKeyScheme)

  private lazy val allRecipientGroups =
    Set(recipientsGroupAlice, recipientsGroupBob, recipientsGroupCharlie)

  private lazy val transactionMetadata
      : Map[ParticipantId, (Recipients, ViewParticipantsKeysAndParentRecipients)] = {
    val parentRecipients = recipientsGroupAlice.recipients
    Map(
      // n1
      bob ->
        (recipientsGroupBob.recipients,
        ViewParticipantsKeysAndParentRecipients(
          NonEmpty.mk(Set, bob),
          Set(Fingerprint.tryCreate("bob_encryptionKey")),
          Some(parentRecipients),
        )),
      // n2
      charlie ->
        (recipientsGroupCharlie.recipients,
        ViewParticipantsKeysAndParentRecipients(
          NonEmpty.mk(Set, charlie),
          Set(Fingerprint.tryCreate("charlie_encryptionKey")),
          Some(parentRecipients),
        )),
      // n0
      alice ->
        (recipientsGroupAlice.recipients,
        ViewParticipantsKeysAndParentRecipients(
          NonEmpty.mk(Set, alice),
          Set(keyFingerprintAlice),
          None,
        )),
    )
  }

  // The ordering is important because for the correct randomness generation to
  // work we need to traverse the tree by pre-order starting with the nodes higher up in the tree.
  private lazy val transactionMetadataOrdered =
    Seq(transactionMetadata(alice), transactionMetadata(bob), transactionMetadata(charlie))

  private def checkReferences(
      randomnessRevocationMap: Map[RecipientGroup, RandomnessRevocationInfo]
  ): Unit =
    randomnessRevocationMap.foreach { case (rg, rri) =>
      if (rg == recipientsGroupAlice)
        rri.encryptedBy shouldBe None
      else
        rri.encryptedBy shouldBe Some(
          randomnessRevocationMap(recipientsGroupAlice).randomnessAndReference.reference
        )
    }

  private def computeRandomnessFromTransaction(
      sessionKeyStoreSnapshot: Map[RecipientGroup, SessionKeyInfo]
  ) = {
    val randomnessRevocationMap = transactionMetadataOrdered
      .foldLeft(Map.empty[RecipientGroup, RandomnessRevocationInfo]) {
        case (state, (recipients, viewMetadata)) =>
          EncryptedViewMessageFactory.computeSessionKeyRandomness(
            sessionKeyStoreSnapshot,
            state,
            recipients,
            viewMetadata,
            pureCrypto,
          )
      }

    checkReferences(randomnessRevocationMap)
    randomnessRevocationMap.keys.toSet shouldBe allRecipientGroups

    randomnessRevocationMap
  }

  private def generateTestRandomness(pureCrypto: CryptoPureApi): SessionKeyAndReference = {
    val randomness =
      pureCrypto.generateSecureRandomness(pureCrypto.defaultSymmetricKeyScheme.keySizeInBytes)
    val sessionKey = pureCrypto
      .createSymmetricKey(randomness, pureCrypto.defaultSymmetricKeyScheme)
      .valueOrFail("encrypt randomness")
    val reference = new Object()

    SessionKeyAndReference(randomness, sessionKey, reference)
  }

  "A EncryptedViewMessageFactory" should {
    "generate the correct randomnesses and references if cache is empty" in {
      computeRandomnessFromTransaction(Map.empty[RecipientGroup, SessionKeyInfo])
      assert(true)
    }

    "generate the correct randomnesses and references if cache is NOT empty" in {

      val keyAndReferenceAlice = generateTestRandomness(pureCrypto)
      val keyAndReferenceBob = generateTestRandomness(pureCrypto)

      val sessionKeyStoreSnapshot: Map[RecipientGroup, SessionKeyInfo] = Map(
        recipientsGroupAlice ->
          SessionKeyInfo(keyAndReferenceAlice, None, Seq.empty),
        recipientsGroupBob ->
          SessionKeyInfo(keyAndReferenceBob, Some(keyAndReferenceAlice.reference), Seq.empty),
      )

      val randomnessRevocationMap = computeRandomnessFromTransaction(sessionKeyStoreSnapshot)

      val rriAlice = randomnessRevocationMap(recipientsGroupAlice)
      val rriBob = randomnessRevocationMap(recipientsGroupBob)

      // randomness should be the same as the one in the initial cache
      rriBob.randomnessAndReference shouldBe
        RandomnessAndReference(keyAndReferenceBob.randomness, keyAndReferenceBob.reference)

      rriAlice.randomnessAndReference shouldBe
        RandomnessAndReference(keyAndReferenceAlice.randomness, keyAndReferenceAlice.reference)
    }

    "revoke the randomness if key is no longer active" in {

      val keyAndReferenceAlice = generateTestRandomness(pureCrypto)
      val keyAndReferenceBob = generateTestRandomness(pureCrypto)

      val sessionKeyStoreSnapshot: Map[RecipientGroup, SessionKeyInfo] = Map(
        recipientsGroupAlice ->
          SessionKeyInfo(
            keyAndReferenceAlice,
            None,
            Seq.empty,
          ),
        recipientsGroupBob ->
          SessionKeyInfo(
            keyAndReferenceBob,
            Some(keyAndReferenceAlice.reference),
            Seq(
              AsymmetricEncrypted(
                ByteString.empty(),
                EciesHkdfHmacSha256Aes128Cbc,
                Fingerprint.tryCreate("wrong_fingerprint"),
              )
            ),
          ),
      )

      val randomnessRevocationMap = computeRandomnessFromTransaction(sessionKeyStoreSnapshot)

      val rriAlice = randomnessRevocationMap(recipientsGroupAlice)
      val rriBob = randomnessRevocationMap(recipientsGroupBob)

      // Bob's key is revoked so the randomness should be different from the one in the initial cache
      rriBob.randomnessAndReference.randomness should not equal keyAndReferenceBob.randomness
      rriBob.randomnessAndReference.reference should not equal keyAndReferenceBob.reference

      rriAlice.randomnessAndReference shouldBe
        RandomnessAndReference(keyAndReferenceAlice.randomness, keyAndReferenceAlice.reference)
    }

    "revoke randomness if parent randomness does not match" in {

      val keyAndReferenceAlice = generateTestRandomness(pureCrypto)
      val keyAndReferenceBob = generateTestRandomness(pureCrypto)

      val sessionKeyStoreSnapshot: Map[RecipientGroup, SessionKeyInfo] = Map(
        recipientsGroupAlice ->
          SessionKeyInfo(
            keyAndReferenceAlice,
            None,
            Seq(
              AsymmetricEncrypted(
                ByteString.empty(),
                EciesHkdfHmacSha256Aes128Cbc,
                Fingerprint.tryCreate("wrong_fingerprint"),
              )
            ),
          ),
        recipientsGroupBob ->
          SessionKeyInfo(
            keyAndReferenceBob,
            Some(keyAndReferenceAlice.reference),
            Seq.empty,
          ),
      )

      val randomnessRevocationMap = computeRandomnessFromTransaction(sessionKeyStoreSnapshot)

      val rriBob = randomnessRevocationMap(recipientsGroupBob)
      val rriAlice = randomnessRevocationMap(recipientsGroupAlice)

      // Alice's key is revoked so both views with alice and bob should have their randomness revoked. The view with
      // alice because of the new key and the view with Bob because the reference to its parent's
      // session key has changed.
      rriBob.randomnessAndReference.randomness should not equal keyAndReferenceBob.randomness
      rriBob.randomnessAndReference.reference should not equal keyAndReferenceBob.reference

      rriAlice.randomnessAndReference.randomness should not equal keyAndReferenceAlice.randomness
      rriAlice.randomnessAndReference.reference should not equal keyAndReferenceAlice.reference
    }

    "compute the correct randomness with multiple transactions" in {

      val keyAndReferenceAlice = generateTestRandomness(pureCrypto)
      val keyAndReferenceBob = generateTestRandomness(pureCrypto)

      val sessionKeyStoreSnapshot: Map[RecipientGroup, SessionKeyInfo] = Map(
        recipientsGroupAlice ->
          SessionKeyInfo(
            keyAndReferenceAlice,
            None,
            Seq(
              AsymmetricEncrypted(
                ByteString.empty(),
                EciesHkdfHmacSha256Aes128Cbc,
                Fingerprint.tryCreate("wrong_fingerprint"),
              )
            ),
          ),
        recipientsGroupBob ->
          SessionKeyInfo(
            keyAndReferenceBob,
            Some(keyAndReferenceAlice.reference),
            Seq(
              AsymmetricEncrypted(
                ByteString.empty(),
                EciesHkdfHmacSha256Aes128Cbc,
                Fingerprint.tryCreate("wrong_fingerprint"),
              )
            ),
          ),
      )

      val randomnessRevocationMap = EncryptedViewMessageFactory.computeSessionKeyRandomness(
        sessionKeyStoreSnapshot,
        Map.empty[RecipientGroup, RandomnessRevocationInfo],
        recipientsGroupAlice.recipients,
        transactionMetadata(alice)._2,
        pureCrypto,
      )

      randomnessRevocationMap.size shouldBe 1

      val rriAlice = randomnessRevocationMap(recipientsGroupAlice)

      rriAlice.randomnessAndReference.randomness should not equal keyAndReferenceAlice.randomness
      rriAlice.randomnessAndReference.reference should not equal keyAndReferenceAlice.reference

      val sessionKeyStoreSnapshotUpdate =
        sessionKeyStoreSnapshot + (recipientsGroupAlice -> SessionKeyInfo(
          SessionKeyAndReference(
            rriAlice.randomnessAndReference.randomness,
            pureCrypto
              .createSymmetricKey(
                rriAlice.randomnessAndReference.randomness,
                pureCrypto.defaultSymmetricKeyScheme,
              )
              .valueOrFail("encrypt randomness"),
            rriAlice.randomnessAndReference.reference,
          ),
          rriAlice.encryptedBy,
          Seq(
            AsymmetricEncrypted(
              ByteString.empty(),
              EciesHkdfHmacSha256Aes128Cbc,
              keyFingerprintAlice,
            )
          ),
        ))

      val randomnessRevocationMapUpdated =
        computeRandomnessFromTransaction(sessionKeyStoreSnapshotUpdate)

      val rriBob = randomnessRevocationMapUpdated(recipientsGroupBob)
      val rriAliceNew = randomnessRevocationMapUpdated(recipientsGroupAlice)

      rriBob.randomnessAndReference.randomness should not equal keyAndReferenceBob.randomness
      rriBob.randomnessAndReference.reference should not equal keyAndReferenceBob.reference

      rriAliceNew.randomnessAndReference shouldBe rriAlice.randomnessAndReference
      rriAliceNew.newKey shouldBe false
    }
  }
}
