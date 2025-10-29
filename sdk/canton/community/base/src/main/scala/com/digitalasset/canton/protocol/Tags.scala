// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Order
import cats.syntax.either.*
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{DeserializationError, HasCryptographicEvidence}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{ByteStringUtil, HexString}
import com.digitalasset.canton.{LedgerTransactionId, ProtoDeserializationError, ReassignmentCounter}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

import scala.collection.mutable.ArrayBuffer

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
final case class UpdateId(private val hash: Hash) extends HasCryptographicEvidence {
  def unwrap: Hash = hash

  def toRootHash: RootHash = RootHash(hash)

  def toProtoPrimitive: ByteString = getCryptographicEvidence

  override def getCryptographicEvidence: ByteString = hash.getCryptographicEvidence

  def asLedgerTransactionId: Either[String, LedgerTransactionId] =
    LedgerTransactionId.fromString(hash.toHexString)

  def tryAsLedgerTransactionId: LedgerTransactionId =
    LedgerTransactionId.assertFromString(hash.toHexString)

  def toHexString: String = hash.toHexString
}

object UpdateId {

  /** The all-zeros transaction ID. This transaction ID is used as the creating transaction ID for
    * contracts whose creation transaction ID is unknown.
    */
  val zero: UpdateId = {
    val algo = HashAlgorithm.Sha256
    new UpdateId(
      Hash.tryFromByteStringRaw(ByteString.copyFrom(new Array[Byte](algo.length.toInt)), algo)
    )
  }

  def fromProtoPrimitive(bytes: ByteString): ParsingResult[UpdateId] =
    Hash
      .fromByteString(bytes)
      .bimap(ProtoDeserializationError.CryptoDeserializationError.apply, UpdateId(_))

  def tryFromProtoPrimitive(bytes: ByteString): UpdateId =
    fromProtoPrimitive(bytes).valueOr(err => throw new IllegalArgumentException(err.toString))

  def tryFromByteArray(bytes: Array[Byte]): UpdateId =
    fromProtoPrimitive(ByteString.copyFrom(bytes)).valueOr(err =>
      throw new IllegalArgumentException(err.toString)
    )

  def fromRootHash(rootHash: RootHash): UpdateId = UpdateId(rootHash.unwrap)

  def fromLedgerString(txId: String): Either[DeserializationError, UpdateId] =
    Hash.fromHexString(txId).map(UpdateId.apply)

  /** Ordering for [[UpdateId]]s based on the serialized hash */
  implicit val orderTransactionId: Order[UpdateId] =
    Order.by[UpdateId, ByteString](_.hash.getCryptographicEvidence)(
      ByteStringUtil.orderByteString
    )

  implicit val orderingTransactionId: Ordering[UpdateId] = orderTransactionId.toOrdering

  implicit val prettyTransactionId: Pretty[UpdateId] = {
    import Pretty.*
    prettyOfParam(_.hash)
  }

  implicit val setParameterTransactionId: SetParameter[UpdateId] = (v, pp) => pp.>>(v.hash)

  implicit val getResultTransactionId: GetResult[UpdateId] = GetResult { r =>
    UpdateId(r.<<)
  }

  implicit val setParameterOptionTransactionId: SetParameter[Option[UpdateId]] = (v, pp) =>
    pp.>>(v.map(_.hash))

  implicit val getResultOptionTransactionId: GetResult[Option[UpdateId]] = GetResult { r =>
    (r.<<[Option[Hash]]).map(UpdateId(_))
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

sealed abstract class ReassignmentId extends PrettyPrinting {
  protected val version: Byte
  protected val payload: ByteString

  def toBytes: ByteString = {
    val buf = new ArrayBuffer[Byte](1 + payload.size)
    buf += version
    buf ++= payload.toByteArray
    ByteString.copyFrom(buf.toArray)
  }

  def toProtoPrimitive: String = HexString.toHexString(toBytes)
  def toProtoV30: v30.ReassignmentId = v30.ReassignmentId(id = toProtoPrimitive)

  @VisibleForTesting
  override protected def pretty: Pretty[ReassignmentId] =
    prettyOfString(rid => s"ReassignmentId(${rid.toProtoPrimitive})")
}

object ReassignmentId {

  def create(hex: String): Either[String, ReassignmentId] =
    HexString
      .parseToByteString(hex)
      .toRight("invalid hex")
      .flatMap(fromBytes)
      .leftMap(err => s"invalid ReassignmentId($hex): $err")

  def tryCreate(str: String): ReassignmentId =
    create(str).valueOr(err => throw new IllegalArgumentException(err))

  def fromProtoPrimitive(str: String): ParsingResult[ReassignmentId] =
    create(str).leftMap(ProtoDeserializationError.StringConversionError(_))

  def fromProtoV30(reassignmentIdP: v30.ReassignmentId): ParsingResult[ReassignmentId] =
    fromProtoPrimitive(reassignmentIdP.id)

  def fromBytes(bytes: ByteString): Either[String, ReassignmentId] =
    if (bytes.isEmpty) Left("no ReassignmentId version")
    else
      (bytes.byteAt(0) match {
        case V0.version => Right(V0(bytes.substring(1)))
        case b => Left(s"invalid version: ${b.toInt}")
      }).leftMap(err => s"cannot parse ReassignmentId bytes: $err")

  def assertFromBytes(bytes: Array[Byte]): ReassignmentId =
    ReassignmentId.fromBytes(ByteString.copyFrom(bytes)) match {
      case Left(e) => throw new IllegalArgumentException(s"Cannot convert reassignment id: $e")
      case Right(id) => id
    }

  def apply(
      source: Source[SynchronizerId],
      target: Target[SynchronizerId],
      unassignmentTs: CantonTimestamp,
      contractIdCounters: Iterable[(LfContractId, ReassignmentCounter)],
  ): ReassignmentId = V0(source, target, unassignmentTs, contractIdCounters.toMap)

  def single(
      source: Source[SynchronizerId],
      target: Target[SynchronizerId],
      unassignmentTs: CantonTimestamp,
      contractId: LfContractId,
      reassignmentCounter: ReassignmentCounter,
  ): ReassignmentId = apply(source, target, unassignmentTs, Seq((contractId, reassignmentCounter)))

  final case class V0 private[ReassignmentId] (override val payload: ByteString)
      extends ReassignmentId {
    override val version = V0.version
  }

  object V0 {
    private[ReassignmentId] val version: Byte = 0x00

    def apply(
        source: Source[SynchronizerId],
        target: Target[SynchronizerId],
        unassignmentTs: CantonTimestamp,
        contractIdCounters: Map[LfContractId, ReassignmentCounter],
    ): ReassignmentId = {
      val builder = Hash.build(HashPurpose.ReassignmentId, HashAlgorithm.Sha256)
      builder.add(source.unwrap.toProtoPrimitive)
      builder.add(target.unwrap.toProtoPrimitive)
      builder.add(unassignmentTs.toProtoPrimitive)
      contractIdCounters.view.toSeq.sortBy(_._1.coid).foreach {
        case (contractId, reassignmentCounter) =>
          builder.add(contractId.coid)
          builder.add(reassignmentCounter.toProtoPrimitive)
      }
      V0(builder.finish().getCryptographicEvidence)
    }
  }

  implicit val getResultReassignmentId: GetResult[ReassignmentId] =
    GetResult(r => tryCreate(r.nextString()))
  implicit val setResultReassignmentId: SetParameter[ReassignmentId] = (v, pp) =>
    pp >> v.toProtoPrimitive
}
