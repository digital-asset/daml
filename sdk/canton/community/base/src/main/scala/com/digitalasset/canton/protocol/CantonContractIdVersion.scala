// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
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
    if (protocolVersion >= ProtocolVersion.v34) Right(AuthenticatedContractIdVersionV11)
    else Left(s"No contract ID scheme found for ${protocolVersion.v}")

  def extractCantonContractIdVersion(
      contractId: LfContractId
  ): Either[MalformedContractId, CantonContractIdVersion] = {
    val LfContractId.V1(_, suffix) = contractId
    for {
      versionedContractId <- CantonContractIdVersion
        .fromContractSuffix(suffix)
        .leftMap(error => MalformedContractId(contractId.toString, error))

      unprefixedSuffix = suffix.slice(versionPrefixBytesSize, suffix.length)

      _ <- Hash
        .fromByteString(unprefixedSuffix.toByteString)
        .leftMap(err => MalformedContractId(contractId.toString, err.message))
    } yield versionedContractId
  }

  // Only use when the contract has been authenticated
  def tryCantonContractIdVersion(contractId: LfContractId): CantonContractIdVersion =
    extractCantonContractIdVersion(contractId).valueOr { err =>
      throw new IllegalArgumentException(
        s"Unable to unwrap object of type ${getClass.getSimpleName}: $err"
      )
    }

  def fromContractSuffix(contractSuffix: Bytes): Either[String, CantonContractIdVersion] =
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

sealed abstract class CantonContractIdVersion(val v: NonNegativeInt)
    extends Ordered[CantonContractIdVersion]
    with Serializable
    with Product {
  require(
    versionPrefixBytes.length == CantonContractIdVersion.versionPrefixBytesSize,
    s"Version prefix of size ${versionPrefixBytes.length} should have size ${CantonContractIdVersion.versionPrefixBytesSize}",
  )

  /** Set to true if upgrade friendly hashing should be used when constructing the contract hash */
  def useUpgradeFriendlyHashing: Boolean

  def versionPrefixBytes: Bytes

  def fromDiscriminator(discriminator: LfHash, unicum: Unicum): LfContractId.V1 =
    LfContractId.V1(discriminator, unicum.toContractIdSuffix(this))

  override final def compare(that: CantonContractIdVersion): Int = this.v.compare(that.v)

}

case object AuthenticatedContractIdVersionV10
    extends CantonContractIdVersion(NonNegativeInt.tryCreate(10)) {
  lazy val versionPrefixBytes: Bytes = Bytes.fromByteArray(Array(0xca.toByte, 0x10.toByte))
  override val useUpgradeFriendlyHashing: Boolean = false
}

case object AuthenticatedContractIdVersionV11
    extends CantonContractIdVersion(NonNegativeInt.tryCreate(11)) {
  lazy val versionPrefixBytes: Bytes = Bytes.fromByteArray(Array(0xca.toByte, 0x11.toByte))
  override val useUpgradeFriendlyHashing: Boolean = true
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
