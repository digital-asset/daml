// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.Bytes
import com.google.protobuf.ByteString

object CantonContractIdVersion {
  val versionPrefixBytesSize = 2

  /** Maximum contract-id-version supported in protocol-version */
  def maximumSupportedVersion(
      protocolVersion: ProtocolVersion
  ): Either[String, CantonContractIdVersion] =
    // TODO(#23971) Use V2 by default in dev
    if (protocolVersion >= ProtocolVersion.v34) Right(AuthenticatedContractIdVersionV11)
    else Left(s"No contract ID scheme found for ${protocolVersion.v}")

  def extractCantonContractIdV1Version(
      contractId: LfContractId
  ): Either[String, CantonContractIdV1Version] =
    extractCantonContractIdVersion(contractId)
      .leftMap(e => s"Contract id is malformed: $e")
      .flatMap {
        case c: CantonContractIdV1Version => Right(c)
        case other => Left(s"Expected CantonContractIdV1Version, got: $other")
      }

  def extractCantonContractIdVersion(
      contractId: LfContractId
  ): Either[MalformedContractId, CantonContractIdVersion] =
    for {
      suffix <- contractId match {
        case LfContractId.V1(_, suffix) => Right(suffix)
        case _ => Left(MalformedContractId(contractId.coid, s"Expected V1 contract id"))
      }
      versionedContractId <- CantonContractIdVersion
        .fromContractSuffix(suffix)
        .leftMap(error => MalformedContractId(contractId.toString, error))

      unprefixedSuffix = suffix.slice(versionPrefixBytesSize, suffix.length)

      _ <- Hash
        .fromByteString(unprefixedSuffix.toByteString)
        .leftMap(err => MalformedContractId(contractId.toString, err.message))
    } yield versionedContractId

  // Only use when the contract has been authenticated
  def tryCantonContractIdVersion(contractId: LfContractId): CantonContractIdVersion =
    extractCantonContractIdVersion(contractId).valueOr { err =>
      throw new IllegalArgumentException(
        s"Unable to unwrap object of type ${getClass.getSimpleName}: $err"
      )
    }

  private def fromContractSuffix(contractSuffix: Bytes): Either[String, CantonContractIdVersion] =
    if (contractSuffix.startsWith(AuthenticatedContractIdVersionV11.versionPrefixBytes)) {
      Right(AuthenticatedContractIdVersionV11)
    } else if (contractSuffix.startsWith(AuthenticatedContractIdVersionV10.versionPrefixBytes)) {
      Right(AuthenticatedContractIdVersionV10)
    } else {
      Left(
        s"Suffix ${contractSuffix.toHexString} is not a supported contract-id prefix"
      )
    }
}

sealed trait CantonContractIdVersion
    extends Ordered[CantonContractIdVersion]
    with Serializable
    with Product {

  type AuthenticationData <: ContractAuthenticationData

  protected def comparisonKey: Int

  override final def compare(that: CantonContractIdVersion): Int =
    this.comparisonKey.compare(that.comparisonKey)
}

sealed abstract class CantonContractIdV1Version(
    override protected val comparisonKey: Int
) extends CantonContractIdVersion {
  require(
    versionPrefixBytes.length == CantonContractIdVersion.versionPrefixBytesSize,
    s"Version prefix of size ${versionPrefixBytes.length} should have size ${CantonContractIdVersion.versionPrefixBytesSize}",
  )

  override type AuthenticationData = ContractAuthenticationDataV1

  def contractHashingMethod: LfHash.HashingMethod

  /** Set to true if upgrade friendly hashing should be used when constructing the contract hash */
  def useUpgradeFriendlyHashing: Boolean =
    contractHashingMethod == LfHash.HashingMethod.UpgradeFriendly

  def versionPrefixBytes: Bytes

  def fromDiscriminator(discriminator: LfHash, unicum: Unicum): LfContractId.V1 =
    LfContractId.V1(discriminator, unicum.toContractIdSuffix(this))
}

case object AuthenticatedContractIdVersionV10 extends CantonContractIdV1Version(10) {
  lazy val versionPrefixBytes: Bytes = Bytes.fromByteArray(Array(0xca.toByte, 0x10.toByte))
  override def contractHashingMethod: LfHash.HashingMethod = LfHash.HashingMethod.Legacy
}

case object AuthenticatedContractIdVersionV11 extends CantonContractIdV1Version(11) {
  lazy val versionPrefixBytes: Bytes = Bytes.fromByteArray(Array(0xca.toByte, 0x11.toByte))
  override def contractHashingMethod: LfHash.HashingMethod = LfHash.HashingMethod.UpgradeFriendly
}

sealed trait CantonContractIdV2Version extends CantonContractIdVersion {
  override type AuthenticationData = ContractAuthenticationDataV2
}

case object CantonContractIdV2Version0 extends CantonContractIdV2Version {
  override protected def comparisonKey: Int = 256
}

object ContractIdSyntax {
  implicit class LfContractIdSyntax(private val contractId: LfContractId) extends AnyVal {
    def toProtoPrimitive: String = contractId.coid

    /** An [[LfContractId]] consists of
      *   - a version (1 byte)
      *   - a discriminator (32 bytes)
      *   - a suffix (at most 94 bytes) Those 1 + 32 + 94 = 127 bytes are base-16 encoded, so this
      *     makes 254 chars at most. See
      *     https://github.com/digital-asset/daml/blob/main/daml-lf/spec/contract-id.rst
      */
    def toLengthLimitedString: String255 = checked(String255.tryCreate(contractId.coid))
    def encodeDeterministically: ByteString = ByteString.copyFromUtf8(toProtoPrimitive)
  }

  implicit val orderingLfContractId: Ordering[LfContractId] =
    Ordering.by[LfContractId, String](_.coid)
}

final case class MalformedContractId(id: String, message: String) {
  override def toString: String =
    s"malformed contract id '$id'" + (if (message.nonEmpty) s". $message" else "")
}
