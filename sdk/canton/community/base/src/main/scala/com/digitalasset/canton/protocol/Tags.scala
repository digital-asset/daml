// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Order
import cats.syntax.either.*
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{DeserializationError, HasCryptographicEvidence}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ByteStringUtil
import com.digitalasset.canton.util.ReassignmentTag.Source
import com.digitalasset.canton.{LedgerTransactionId, ProtoDeserializationError}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

/** The root hash of a Merkle tree used as an identifier for requests.
  *
  * Extends [[com.digitalasset.canton.serialization.HasCryptographicEvidence]] so that
  * [[RootHash]]'s serialization can be used to compute the hash of an inner Merkle node from its
  * children using [[RootHash.getCryptographicEvidence]]. Serialization to Protobuf fields can be
  * done with [[RootHash.toProtoPrimitive]]
  *
  * Here is how we use it:
  *   1. Every participant gets a “partially blinded” Merkle tree, defining the locations of the
  *      views they are privy to.
  *   1. That Merkle tree has a root. That root has a hash. That’s the root hash.
  *   1. The mediator receives a fully blinded Merkle tree, with the same hash.
  *   1. The submitting participant will send for each receiving participant an additional “root
  *      hash message” in the same batch. That message will contain the same hash, with recipients
  *      (participant, mediator).
  *   1. The mediator will check that all participants mentioned in the tree received a root hash
  *      message and that all hashes are equal.
  *   1. Once the mediator sends out the verdict, the verdict will include the tree structure and
  *      thus the root hash. Hence, the participant will now have certainty about the mediator
  *      having checked all root hash messages and having observed the same tree structure.
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class RootHash(private val hash: Hash) extends PrettyPrinting with HasCryptographicEvidence {
  def unwrap: Hash = hash

  override def getCryptographicEvidence: ByteString = hash.getCryptographicEvidence

  def toProtoPrimitive: ByteString = getCryptographicEvidence

  def asLedgerTransactionId: Either[String, LedgerTransactionId] =
    LedgerTransactionId.fromString(hash.toHexString)

  override protected def pretty: Pretty[RootHash] = prettyOfParam(_.unwrap)
}

object RootHash {
  implicit val setParameterRootHash: SetParameter[RootHash] = (rh, pp) =>
    pp >> rh.unwrap.toLengthLimitedHexString

  implicit val getResultRootHash: GetResult[RootHash] = GetResult { r =>
    RootHash(Hash.tryFromHexString(r.<<))
  }

  implicit val setParameterRootHashO: SetParameter[Option[RootHash]] = (rh, pp) =>
    pp >> rh.map(_.unwrap.toLengthLimitedHexString)

  implicit val getResultRootHashO: GetResult[Option[RootHash]] = { r =>
    r.<<[Option[String]].map(Hash.tryFromHexString).map(RootHash(_))
  }

  def fromByteString(bytes: ByteString): Either[DeserializationError, RootHash] =
    Hash.fromByteString(bytes).map(RootHash(_))

  def fromProtoPrimitive(bytes: ByteString): ParsingResult[RootHash] =
    Hash.fromProtoPrimitive(bytes).map(RootHash(_))

  def fromProtoPrimitiveOption(
      bytes: ByteString
  ): ParsingResult[Option[RootHash]] =
    Hash.fromProtoPrimitiveOption(bytes).map(_.map(RootHash(_)))
}

/** A hash-based transaction id. */
final case class TransactionId(private val hash: Hash) extends HasCryptographicEvidence {
  def unwrap: Hash = hash

  def toRootHash: RootHash = RootHash(hash)

  def toProtoPrimitive: ByteString = getCryptographicEvidence

  override def getCryptographicEvidence: ByteString = hash.getCryptographicEvidence

  def asLedgerTransactionId: Either[String, LedgerTransactionId] =
    LedgerTransactionId.fromString(hash.toHexString)

  def tryAsLedgerTransactionId: LedgerTransactionId =
    LedgerTransactionId.assertFromString(hash.toHexString)
}

object TransactionId {

  def fromProtoPrimitive(bytes: ByteString): ParsingResult[TransactionId] =
    Hash
      .fromByteString(bytes)
      .bimap(ProtoDeserializationError.CryptoDeserializationError.apply, TransactionId(_))

  def fromRootHash(rootHash: RootHash): TransactionId = TransactionId(rootHash.unwrap)

  /** Ordering for [[TransactionId]]s based on the serialized hash */
  implicit val orderTransactionId: Order[TransactionId] =
    Order.by[TransactionId, ByteString](_.hash.getCryptographicEvidence)(
      ByteStringUtil.orderByteString
    )

  implicit val orderingTransactionId: Ordering[TransactionId] = orderTransactionId.toOrdering

  implicit val prettyTransactionId: Pretty[TransactionId] = {
    import Pretty.*
    prettyOfParam(_.unwrap)
  }

  implicit val setParameterTransactionId: SetParameter[TransactionId] = (v, pp) => pp.>>(v.hash)

  implicit val getResultTransactionId: GetResult[TransactionId] = GetResult { r =>
    TransactionId(r.<<)
  }

  implicit val setParameterOptionTransactionId: SetParameter[Option[TransactionId]] = (v, pp) =>
    pp.>>(v.map(_.hash))

  implicit val getResultOptionTransactionId: GetResult[Option[TransactionId]] = GetResult { r =>
    (r.<<[Option[Hash]]).map(TransactionId(_))
  }
}

/** A hash-based transaction view id
  *
  * Views from different requests may have the same view hash.
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class ViewHash(private val hash: Hash) extends PrettyPrinting {
  def unwrap: Hash = hash

  def toProtoPrimitive: ByteString = hash.getCryptographicEvidence

  def toRootHash: RootHash = RootHash(hash)

  @VisibleForTesting
  override def pretty: Pretty[ViewHash] = prettyOfClass(unnamedParam(_.hash))
}

object ViewHash {

  def fromProtoPrimitive(hash: ByteString): ParsingResult[ViewHash] =
    Hash.fromProtoPrimitive(hash).map(ViewHash(_))

  def fromProtoPrimitiveOption(
      hash: ByteString
  ): ParsingResult[Option[ViewHash]] =
    Hash.fromProtoPrimitiveOption(hash).map(_.map(ViewHash(_)))

  def fromRootHash(hash: RootHash): ViewHash = ViewHash(hash.unwrap)

  /** Ordering for [[ViewHash]] based on the serialized hash */
  implicit val orderViewHash: Order[ViewHash] =
    Order.by[ViewHash, ByteString](_.hash.getCryptographicEvidence)(ByteStringUtil.orderByteString)
}

/** A confirmation request is identified by the sequencer timestamp. */
final case class RequestId(private val ts: CantonTimestamp) extends PrettyPrinting {
  def unwrap: CantonTimestamp = ts

  def toProtoPrimitive: Long = ts.toProtoPrimitive

  override protected def pretty: Pretty[RequestId] = prettyOfClass(unnamedParam(_.ts))
}

object RequestId {
  implicit val requestIdOrdering: Ordering[RequestId] =
    Ordering.by[RequestId, CantonTimestamp](_.unwrap)
  implicit val requestIdOrder: Order[RequestId] = Order.fromOrdering[RequestId]

  def fromProtoPrimitive(requestIdP: Long): ParsingResult[RequestId] =
    CantonTimestamp.fromProtoPrimitive(requestIdP).map(RequestId(_))
}

final case class UnassignId(hash: Hash) extends PrettyPrinting {
  def toHexString: String = hash.toHexString
  def toProtoPrimitive: String = toHexString

  @VisibleForTesting
  override protected def pretty: Pretty[UnassignId] = prettyOfClass(unnamedParam(_.hash))
}

object UnassignId {
  def apply(
      source: Source[SynchronizerId],
      timestamp: CantonTimestamp,
  ): UnassignId = {
    val builder = Hash.build(HashPurpose.UnassignId, HashAlgorithm.Sha256)
    builder.add(source.unwrap.toProtoPrimitive)
    builder.add(timestamp.toProtoPrimitive)
    UnassignId(builder.finish())
  }

  def fromHexString(hex: String): Either[DeserializationError, UnassignId] =
    Hash.fromHexString(hex).map(UnassignId(_))

  def tryFromHexString(hex: String): UnassignId =
    fromHexString(hex).valueOr(err =>
      throw new IllegalArgumentException(s"Invalid UnassignId: $err")
    )

  def fromProtoPrimitive(hex: String): ParsingResult[UnassignId] =
    fromHexString(hex).leftMap(ProtoDeserializationError.CryptoDeserializationError(_))

  implicit val getResultUnassignId: GetResult[UnassignId] =
    GetResult(r => UnassignId.tryFromHexString(r.nextString()))

  implicit val setResultUnassignId: SetParameter[UnassignId] = (v, pp) => pp >> v.toHexString
}

/** A reassignment is identified by the unassignHash */
final case class ReassignmentId(
    // TODO(#25483) This should be physical
    sourceSynchronizer: Source[SynchronizerId],
    unassignId: UnassignId,
) extends PrettyPrinting {
  def toProtoV30: v30.ReassignmentId =
    v30.ReassignmentId(
      sourceSynchronizerId = sourceSynchronizer.unwrap.toProtoPrimitive,
      unassignId = unassignId.toProtoPrimitive,
    )

  override protected def pretty: Pretty[ReassignmentId] = prettyOfClass(
    param("unassignId", _.unassignId),
    param("source", _.sourceSynchronizer),
  )
}

object ReassignmentId {
  def fromProtoV30(reassignmentIdP: v30.ReassignmentId): ParsingResult[ReassignmentId] =
    reassignmentIdP match {
      case v30.ReassignmentId(sourceSynchronizerP, unassignmentIdP) =>
        for {
          sourceSynchronizerId <- SynchronizerId.fromProtoPrimitive(
            sourceSynchronizerP,
            "ReassignmentId.source_synchronizer_id",
          )
          unassignId <- UnassignId.fromProtoPrimitive(unassignmentIdP)
        } yield ReassignmentId(Source(sourceSynchronizerId), unassignId)
    }

  def tryCreate(source: Source[SynchronizerId], hex: String): ReassignmentId =
    ReassignmentId(source, UnassignId.tryFromHexString(hex))
}
