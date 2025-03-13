// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.syntax.bifunctor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash

object TopologyTransactionSignature {
  sealed trait TopologyTransactionSignatureError
  final case class MultiHashSignatureDoesNotCoverTransaction(
      requiredHash: TxHash,
      providedHashes: NonEmpty[Set[TxHash]],
      signature: Signature,
  ) extends TopologyTransactionSignatureError
      with PrettyPrinting {
    override protected def pretty: Pretty[MultiHashSignatureDoesNotCoverTransaction.this.type] =
      prettyOfClass(
        param("requiredHash", _.requiredHash.hash),
        param("providedHashes", _.providedHashes.map(_.hash)),
        param("signedBy", _.signature.signedBy),
      )
  }
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

  def signedBy: Fingerprint
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

  @inline override def signedBy: Fingerprint = signature.signedBy
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

  @inline override def signedBy: Fingerprint = signature.signedBy
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
