// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.Order
import cats.syntax.bifunctor.*
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  DeserializationError,
  HasCryptographicEvidence,
  ProtoConverter,
}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.ByteStringUtil
import com.digitalasset.canton.{LedgerTransactionId, ProtoDeserializationError}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp
import slick.jdbc.{GetResult, SetParameter}

/** The root hash of a Merkle tree used as an identifier for requests.
  *
  * Extends [[com.digitalasset.canton.serialization.HasCryptographicEvidence]] so that [[RootHash]]'s serialization
  * can be used to compute the hash of an inner Merkle node from its children using [[RootHash.getCryptographicEvidence]].
  * Serialization to Protobuf fields can be done with [[RootHash.toProtoPrimitive]]
  *
  * Here is how we use it:
  * (1) Every participant gets a “partially blinded” Merkle tree, defining the locations of the views they are privy to.
  * (2) That Merkle tree has a root. That root has a hash. That’s the root hash.
  * (3) The mediator receives a fully blinded Merkle tree, with the same hash.
  * (4) The submitting participant will send for each receiving participant an additional “root hash message” in the
  *     same batch. That message will contain the same hash, with recipients (participant, mediator).
  * (5) The mediator will check that all participants mentioned in the tree received a root hash message and that all
  *     hashes are equal.
  * (6) Once the mediator sends out the verdict, the verdict will include the tree structure and thus the root hash.
  *     Hence, the participant will now have certainty about the mediator having checked all root hash messages
  *     and having observed the same tree structure.
  */
@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class RootHash(private val hash: Hash) extends PrettyPrinting with HasCryptographicEvidence {
  def unwrap: Hash = hash

  override def getCryptographicEvidence: ByteString = hash.getCryptographicEvidence

  def toProtoPrimitive: ByteString = getCryptographicEvidence

  def asLedgerTransactionId: Either[String, LedgerTransactionId] =
    LedgerTransactionId.fromString(hash.toHexString)

  override def pretty: Pretty[RootHash] = prettyOfParam(_.unwrap)
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
      .bimap(ProtoDeserializationError.CryptoDeserializationError, TransactionId(_))

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

  def toProtoPrimitive: ProtoTimestamp = ts.toProtoPrimitive

  override def pretty: Pretty[RequestId] = prettyOfClass(unnamedParam(_.ts))
}

object RequestId {
  implicit val requestIdOrdering: Ordering[RequestId] = Ordering.by[RequestId, CantonTimestamp](_.unwrap)
  implicit val requestIdOrder: Order[RequestId] = Order.fromOrdering[RequestId]

  def fromProtoPrimitive(requestIdP: ProtoTimestamp): ParsingResult[RequestId] =
    CantonTimestamp.fromProtoPrimitive(requestIdP).map(RequestId(_))
}

/** A transfer is identified by the source domain and the sequencer timestamp on the transfer-out request. */
final case class TransferId(sourceDomain: SourceDomainId, transferOutTimestamp: CantonTimestamp)
    extends PrettyPrinting {
  def toProtoV0: v0.TransferId =
    v0.TransferId(
      originDomain = sourceDomain.toProtoPrimitive,
      timestamp = Some(transferOutTimestamp.toProtoPrimitive),
    )

  override def pretty: Pretty[TransferId] = prettyOfClass(
    param("ts", _.transferOutTimestamp),
    param("source", _.sourceDomain),
  )
}

object TransferId {
  implicit val transferIdGetResult: GetResult[TransferId] = GetResult { r =>
    TransferId(
      SourceDomainId(GetResult[DomainId].apply(r)),
      GetResult[CantonTimestamp].apply(r),
    )
  }

  def fromProtoV0(transferIdP: v0.TransferId): ParsingResult[TransferId] =
    transferIdP match {
      case v0.TransferId(sourceDomainP, requestTimestampP) =>
        for {
          sourceDomain <- DomainId.fromProtoPrimitive(sourceDomainP, "TransferId.origin_domain")
          requestTimestamp <- ProtoConverter
            .required("TransferId.timestamp", requestTimestampP)
            .flatMap(CantonTimestamp.fromProtoPrimitive)
        } yield TransferId(SourceDomainId(sourceDomain), requestTimestamp)
    }
}
