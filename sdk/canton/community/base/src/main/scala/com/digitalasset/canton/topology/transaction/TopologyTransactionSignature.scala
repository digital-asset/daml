// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.syntax.bifunctor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash

object TopologyTransactionSignature {

  /** Returns a set of signatures, such that only one signature per signing key is retained. In case
    * there are multiple signatures by the same signing key, the signature with the lowest index in
    * the sequence is picked for the result.
    */
  def distinctSignatures(
      signatures: NonEmpty[Seq[TopologyTransactionSignature]]
  ): NonEmpty[Set[TopologyTransactionSignature]] =
    signatures.zipWithIndex
      .groupBy1 { case (tx, _) => tx.authorizingLongTermKey }
      .map { case (_, signatures) =>
        signatures.minBy1 { case (_, index) => index }
      }
      .toSeq
      .sortBy { case (_, index) => index }
      .map { case (tx, _) => tx }
      .toSet
}

/** Trait for different types of topology transaction signatures
  */
sealed trait TopologyTransactionSignature extends Product with Serializable {

  /** Computes the hash that should be used to verify the signature
    */
  def computeHashForSignatureVerification(hashOps: HashOps): Hash

  /** Signature of a topology transaction
    */
  def signature: Signature

  /** Hashes covered by the signature
    */
  def hashesCovered: NonEmpty[Set[TxHash]]

  def coversHash(txHash: TxHash): Boolean

  def authorizingLongTermKey: Fingerprint
}

/** Signature over the specific transaction hash
  */
final case class SingleTransactionSignature(
    private val transactionHash: TxHash,
    signature: Signature,
) extends TopologyTransactionSignature {
  override def computeHashForSignatureVerification(hashOps: HashOps): Hash = transactionHash.hash
  override lazy val hashesCovered: NonEmpty[Set[TxHash]] = NonEmpty.mk(Set, transactionHash)

  override def coversHash(txHash: TxHash): Boolean = transactionHash == txHash

  @inline override def authorizingLongTermKey: Fingerprint = signature.authorizingLongTermKey
}

/** Signature over the hash of multiple transaction.
  */
final case class MultiTransactionSignature(
    transactionHashes: NonEmpty[Set[TxHash]],
    signature: Signature,
) extends TopologyTransactionSignature {
  override def computeHashForSignatureVerification(
      hashOps: HashOps
  ): Hash = MultiTransactionSignature.computeCombinedHash(transactionHashes, hashOps)
  override lazy val hashesCovered: NonEmpty[Set[TxHash]] = transactionHashes

  override def coversHash(txHash: TxHash): Boolean = transactionHashes.contains(txHash)

  @inline override def authorizingLongTermKey: Fingerprint = signature.authorizingLongTermKey
}

object MultiTransactionSignature {
  def computeCombinedHash(transactionHashes: NonEmpty[Set[TxHash]], hashOps: HashOps): Hash = {
    val hashBuilder: HashBuilder = hashOps.build(HashPurpose.MultiTopologyTransaction)
    // Sort and prefix with the list size
    val resultBuilder =
      transactionHashes.toSeq.sortBy(_.hash).foldLeft(hashBuilder.add(transactionHashes.size)) {
        case (builder, txHash) =>
          // then add the hashes individually, in order. They'll each be prefixed with their byte size as well
          builder.add(txHash.hash.getCryptographicEvidence)
      }
    resultBuilder.finish()
  }

  def fromProtoV30(
      multiTransactionSignaturesP: v30.MultiTransactionSignatures,
      transactionHash: TxHash,
  ): ParsingResult[NonEmpty[Seq[MultiTransactionSignature]]] = {
    val v30.MultiTransactionSignatures(transactionHashesP, signaturesP) =
      multiTransactionSignaturesP
    for {
      transactionHashes <- ProtoConverter.parseRequiredNonEmpty(
        Hash.fromProtoPrimitive,
        "transaction_hashes",
        transactionHashesP,
      )
      signatures <- ProtoConverter.parseRequiredNonEmpty(
        Signature.fromProtoV30,
        "signatures",
        signaturesP,
      )
      _ <- Either
        .cond(
          transactionHashes.contains(transactionHash.hash),
          (),
          ProtoDeserializationError
            .InvariantViolation("transaction_hashes", "does not contain the transaction hash"),
        )
        .leftWiden[ProtoDeserializationError]
    } yield signatures.map { signature =>
      MultiTransactionSignature(transactionHashes.toSet.map(TxHash(_)), signature)
    }
  }
}
