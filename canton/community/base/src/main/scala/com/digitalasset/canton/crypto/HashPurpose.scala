// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import scala.collection.mutable

/** The purpose of a hash serves to avoid hash collisions due to equal encodings for different objects.
  * It is in general not possible to derive the purpose of the hash from the hash alone.
  *
  * Whenever a hash is computed using [[HashOps]], a [[HashPurpose]] must be specified that gets included in the hash.
  * To reliably prevent hash collisions, every [[HashPurpose]] object should be used only in a single place.
  *
  * All [[HashPurpose]] objects must be created through the [[HashPurpose$.apply]] method, which checks that the id is
  * fresh.
  *
  * @param id The identifier for the [[HashPurpose]].
  *           Every [[HashPurpose]] object must have a unique [[id]].
  */
class HashPurpose private (val id: Int) extends AnyVal

object HashPurpose {
  private val ids: mutable.Map[Int, String] = mutable.TreeMap.empty[Int, String]

  /** Creates a new [[HashPurpose]] with a given description */
  def apply(id: Int, description: String): HashPurpose = {
    ids.put(id, description).foreach { oldDescription =>
      throw new IllegalArgumentException(
        s"requirement failed: HashPurpose with id=$id already exists for $oldDescription"
      )
    }

    new HashPurpose(id)
  }

  /** Returns the description that was given when the hash purpose was created. */
  def description(hashPurpose: HashPurpose): String =
    ids.getOrElse(
      hashPurpose.id,
      throw new IllegalStateException(
        s"Hash purpose with id ${hashPurpose.id} has been created without going through apply"
      ),
    )

  /* HashPurposes are listed as `val` rather than `case object`s such that they are initialized eagerly.
   * This ensures that HashPurpose id clashes are detected eagerly. Otherwise, it may be there are two hash purposes
   * with the same id, but they are never used in the same Java process and therefore the clash is not detected.
   */
  val SequencedEventSignature = HashPurpose(1, "SequencedEventSignature")
  val Hmac = HashPurpose(2, "Hmac")
  val MerkleTreeInnerNode = HashPurpose(3, "MerkleTreeInnerNode")
  val Discriminator = HashPurpose(4, "Discriminator")
  val SubmitterMetadata = HashPurpose(5, "SubmitterMetadata")
  val CommonMetadata = HashPurpose(6, "CommonMetadata")
  val ParticipantMetadata = HashPurpose(7, "ParticipantMetadata")
  val ViewCommonData = HashPurpose(8, "ViewCommonData")
  val ViewParticipantData = HashPurpose(9, "ViewParticipantData")
  val MalformedMediatorRequestResult = HashPurpose(10, "MalformedMediatorRequestResult")
  val TopologyTransactionSignature = HashPurpose(11, "TopologyTransactionSignature")
  val PublicKeyFingerprint = HashPurpose(12, "PublicKeyFingerprint")
  val DarIdentifier = HashPurpose(13, "DarIdentifier")
  val AuthenticationToken = HashPurpose(14, "AuthenticationToken")
  val AgreementId = HashPurpose(15, "AgreementId")
  val MediatorResponseSignature = HashPurpose(16, "MediatorResponseSignature")
  val TransactionResultSignature = HashPurpose(17, "TransactionResultSignature")
  val TransferResultSignature = HashPurpose(19, "TransferResultSignature")
  val ParticipantStateSignature = HashPurpose(20, "ParticipantStateSignature")
  val DomainTopologyTransactionMessageSignature =
    HashPurpose(21, "DomainTopologyTransactionMessageSignature")
  val AcsCommitment = HashPurpose(22, "AcsCommitment")
  val Stakeholders = HashPurpose(23, "Stakeholders")
  val TransferOutCommonData = HashPurpose(24, "TransferOutCommonData")
  val TransferOutView = HashPurpose(25, "TransferOutView")
  val TransferInCommonData = HashPurpose(26, "TransferInCommonData")
  val TransferInView = HashPurpose(27, "TransferInView")
  val TransferViewTreeMessageSeed = HashPurpose(28, "TransferViewTreeMessageSeed")
  val Unicum = HashPurpose(29, "Unicum")
  val RepairTransactionId = HashPurpose(30, "RepairTransactionId")
  val MediatorLeadershipEvent = HashPurpose(31, "MediatorLeadershipEvent")
  val LegalIdentityClaim = HashPurpose(32, "LegalIdentityClaim")
  val DbLockId = HashPurpose(33, "DbLockId")
  val AcsCommitmentDb = HashPurpose(34, "AcsCommitmentDb")
  val SubmissionRequestSignature = HashPurpose(35, "SubmissionRequestSignature")
  val AcknowledgementSignature = HashPurpose(36, "AcknowledgementSignature")
  val DecentralizedNamespaceNamespace = HashPurpose(37, "DecentralizedNamespace")
  val SignedProtocolMessageSignature = HashPurpose(38, "SignedProtocolMessageSignature")
  val AggregationId = HashPurpose(39, "AggregationId")

}
