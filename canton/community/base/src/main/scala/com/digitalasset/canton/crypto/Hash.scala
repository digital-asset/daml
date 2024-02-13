// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.Order
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.CryptoDeserializationError
import com.digitalasset.canton.config.CantonRequireTypes.String68
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  DeterministicEncoding,
  HasCryptographicEvidence,
}
import com.digitalasset.canton.util.HexString
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

/** A Multi-hash compatible description of a hash algorithm
  *
  * @param name Human readable name, must align with JCE hash algorithm names
  * @param index Multi-hash index
  * @param length Length of the hash in bytes
  *
  * NOTE: There exists an upper limit on the supported lengths due to serialization constraints on LF ledger strings.
  * The total length of a multi-hash hash (including the var-int index and length encoding, plus the actual length of
  * the hash) must not exceed 46 bytes.
  *
  * NOTE: there exists a similar, soft upper limit, on the supported lengths due to string length constraints in the
  * database. The total amount of bytes produced by `digest` should not exceed 512 bytes (this will lead to a HexString of
  * length 1024). If needed, this limit can be increased by increasing the allowed characters for varchar's in the DBs.
  */
sealed abstract class HashAlgorithm(val name: String, val index: Long, val length: Long)
    extends PrettyPrinting {
  def toProtoEnum: v30.HashAlgorithm

  override def pretty: Pretty[HashAlgorithm] = prettyOfString(_.name)
}

object HashAlgorithm {

  implicit val hashAlgorithmOrder: Order[HashAlgorithm] = Order.by[HashAlgorithm, Long](_.index)

  val algorithms: Map[Long, HashAlgorithm] = Map {
    0x12L -> Sha256
  }

  case object Sha256 extends HashAlgorithm("SHA-256", 0x12, 32) {
    override def toProtoEnum: v30.HashAlgorithm = v30.HashAlgorithm.HASH_ALGORITHM_SHA256
  }

  def lookup(index: Long, length: Long): Either[String, HashAlgorithm] =
    for {
      algo <- algorithms.get(index).toRight(s"Unknown hash algorithm for index: $index")
      _ <- Either.cond(
        algo.length == length,
        (),
        s"Mismatch of lengths for ${algo.name}: given $length, expected ${algo.length}",
      )
    } yield algo

  def fromProtoEnum(
      field: String,
      hashAlgorithmP: v30.HashAlgorithm,
  ): ParsingResult[HashAlgorithm] =
    hashAlgorithmP match {
      case v30.HashAlgorithm.HASH_ALGORITHM_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.HashAlgorithm.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.HashAlgorithm.HASH_ALGORITHM_SHA256 => Right(Sha256)
    }
}

final case class Hash private (private val hash: ByteString, private val algorithm: HashAlgorithm)
    extends HasCryptographicEvidence
    with Ordered[Hash]
    with PrettyPrinting {

  require(!hash.isEmpty, "Hash must not be empty")
  require(
    hash.size() == algorithm.length,
    s"Hash size ${hash.size()} must match hash algorithm length ${algorithm.length}",
  )

  /** Multi-hash compatible serialization */
  override def getCryptographicEvidence: ByteString =
    DeterministicEncoding
      .encodeUVarInt(algorithm.index)
      .concat(DeterministicEncoding.encodeUVarInt(algorithm.length))
      .concat(hash)

  // A serialization of the entire multi-hash to a hex string
  val toHexString: String = HexString.toHexString(getCryptographicEvidence)
  // We assume/require that the HexString of a hash has at most 68 characters (for reference: outputs of our SHA256 algorithm have
  // exactly 68 HexString characters). If you want to increase this limit, please consult the team, see also documentation at `LengthLimitedString` for more details
  val toLengthLimitedHexString: String68 =
    String68.tryCreate(toHexString, Some("HexString of hash"))

  def compare(that: Hash): Int = {
    this.toHexString.compare(that.toHexString)
  }

  override val pretty: Pretty[Hash] = prettyOfString(hash =>
    s"${hash.algorithm.name}:${HexString.toHexString(hash.hash).readableHash}"
  )

  /** Access to the raw hash, should NOT be used for serialization. */
  private[canton] def unwrap: ByteString = hash
}

object Hash {

  implicit val setParameterHash: SetParameter[Hash] = (hash, pp) => {
    import com.digitalasset.canton.resource.DbStorage.Implicits.setParameterByteString
    pp.>>(hash.getCryptographicEvidence)
  }

  implicit val getResultHash: GetResult[Hash] = GetResult { r =>
    import com.digitalasset.canton.resource.DbStorage.Implicits.getResultByteString
    tryFromByteString(r.<<)
  }

  implicit val setParameterOptionHash: SetParameter[Option[Hash]] = (hash, pp) => {
    import com.digitalasset.canton.resource.DbStorage.Implicits.setParameterByteStringOption
    pp.>>(hash.map(_.getCryptographicEvidence))
  }

  implicit val getResultOptionHash: GetResult[Option[Hash]] = GetResult { r =>
    import com.digitalasset.canton.resource.DbStorage.Implicits.getResultByteStringOption
    (r.<<[Option[ByteString]]).map(bytes => tryFromByteString(bytes))

  }

  private[crypto] def tryCreate(hash: ByteString, algorithm: HashAlgorithm): Hash =
    create(hash, algorithm).valueOr(err => throw new IllegalArgumentException(err))

  private[crypto] def create(hash: ByteString, algorithm: HashAlgorithm): Either[String, Hash] =
    Either.cond(
      hash.size() == algorithm.length,
      new Hash(hash, algorithm),
      s"Size of given hash ${hash.size()} does not match expected size ${algorithm.length} for ${algorithm.name}",
    )

  private def tryFromByteString(bytes: ByteString): Hash =
    fromByteString(bytes).valueOr(err =>
      throw new IllegalArgumentException(s"Failed to deserialize hash from $bytes: $err")
    )

  def build(purpose: HashPurpose, algorithm: HashAlgorithm): HashBuilder =
    new HashBuilderFromMessageDigest(algorithm, purpose)

  def digest(purpose: HashPurpose, bytes: ByteString, algorithm: HashAlgorithm): Hash = {
    // It's safe to use `addWithoutLengthPrefix` because there cannot be hash collisions due to concatenation
    // as we're immediately calling `finish`.
    build(purpose, algorithm).addWithoutLengthPrefix(bytes).finish()
  }

  def fromProtoPrimitive(bytes: ByteString): ParsingResult[Hash] =
    fromByteString(bytes).leftMap(CryptoDeserializationError)

  def fromProtoPrimitiveOption(bytes: ByteString): ParsingResult[Option[Hash]] =
    fromByteStringOption(bytes).leftMap(CryptoDeserializationError)

  /** Decode a serialized [[Hash]] from a multi-hash format. */
  def fromByteString(bytes: ByteString): Either[DeserializationError, Hash] =
    for {
      indexAndBytes <- DeterministicEncoding.decodeUVarInt(bytes)
      (index, lengthAndHashBytes) = indexAndBytes
      lengthAndBytes <- DeterministicEncoding.decodeUVarInt(lengthAndHashBytes)
      (length, hashBytes) = lengthAndBytes
      algorithm <- HashAlgorithm
        .lookup(index, length)
        .leftMap(err => DefaultDeserializationError(s"Invalid hash algorithm: $err"))
      hash <- create(hashBytes, algorithm).leftMap(err => DefaultDeserializationError(err))
    } yield hash

  /** Decode a serialized [[Hash]] using [[fromByteString]] except for the empty [[com.google.protobuf.ByteString]],
    * which maps to [[scala.None$]].
    */
  def fromByteStringOption(bytes: ByteString): Either[DeserializationError, Option[Hash]] =
    if (bytes.isEmpty) Right(None) else fromByteString(bytes).map(Some(_))

  def fromHexString(hexString: String): Either[DeserializationError, Hash] =
    HexString
      .parse(hexString)
      .toRight(DefaultDeserializationError(s"Failed to parse hex string: $hexString"))
      .map(ByteString.copyFrom)
      .flatMap(fromByteString)

  def tryFromHexString(hexString: String): Hash =
    fromHexString(hexString).valueOr(err =>
      throw new IllegalArgumentException(s"Invalid hex string: $err")
    )
}

/** Trait only needed if we want to make the default algorithm configurable
  */
trait HashOps {

  def defaultHashAlgorithm: HashAlgorithm

  /** Creates a [[HashBuilder]] for computing a hash with the given purpose.
    * For different purposes `purpose1` and `purpose2`, all implementations must ensure
    * that it is computationally infeasible to find a sequence `bs` of [[com.google.protobuf.ByteString]]s
    * such that `bs.foldLeft(hashBuilder(purpose1))((b, hb) => hb.add(b)).finish`
    * and `bs.foldLeft(hashBuilder(purpose2))((b, hb) => hb.add(b)).finish`
    * yield the same hash.
    */
  def build(purpose: HashPurpose, algorithm: HashAlgorithm = defaultHashAlgorithm): HashBuilder =
    Hash.build(purpose, algorithm)

  /** Convenience method for `build(purpose).addWithoutLengthPrefix(bytes).finish` */
  def digest(
      purpose: HashPurpose,
      bytes: ByteString,
      algorithm: HashAlgorithm = defaultHashAlgorithm,
  ): Hash =
    Hash.digest(purpose, bytes, algorithm)
}
