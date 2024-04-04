// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.daml.ledger.javaapi.data.codegen.ContractId
import com.daml.lf.data.Bytes
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.ledger.api.refinements.ApiTypes
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

object CantonContractIdVersion {
  val versionPrefixBytesSize = 2

  def fromProtocolVersion(protocolVersion: ProtocolVersion): CantonContractIdVersion =
    protocolVersion match {
      case protocolVersion if protocolVersion >= ProtocolVersion.v6 =>
        AuthenticatedContractIdV3
      case ProtocolVersion.v5 => AuthenticatedContractIdV2
      case ProtocolVersion.v4 => AuthenticatedContractIdV1
      case _olderProtocolVersions => NonAuthenticatedContractId
    }

  def ensureCantonContractId(
      contractId: LfContractId
  ): Either[MalformedContractId, CantonContractIdVersion] = {
    val LfContractId.V1(_discriminator, suffix) = contractId
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

  def fromContractSuffix(contractSuffix: Bytes): Either[String, CantonContractIdVersion] = {
    def invalidContractIdVersionPrefix = Left(
      s"""Suffix ${contractSuffix.toHexString} does not start with one of the supported prefixes:
         | ${AuthenticatedContractIdV3.versionPrefixBytes},
         | ${AuthenticatedContractIdV2.versionPrefixBytes},
         | ${AuthenticatedContractIdV1.versionPrefixBytes}
         | or ${NonAuthenticatedContractId.versionPrefixBytes}
         |""".stripMargin.replaceAll("\r|\n", "")
    )

    if (contractSuffix.length < versionPrefixBytesSize)
      invalidContractIdVersionPrefix
    else
      contractSuffix.slice(0, versionPrefixBytesSize) match {
        case AuthenticatedContractIdV3.versionPrefixBytes =>
          Right(AuthenticatedContractIdV3)
        case AuthenticatedContractIdV2.versionPrefixBytes =>
          Right(AuthenticatedContractIdV2)
        case AuthenticatedContractIdV1.versionPrefixBytes =>
          Right(AuthenticatedContractIdV1)
        case NonAuthenticatedContractId.versionPrefixBytes =>
          Right(NonAuthenticatedContractId)
        case _other => invalidContractIdVersionPrefix
      }
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

  def isAuthenticated: Boolean

  def versionPrefixBytes: Bytes

  def fromDiscriminator(discriminator: LfHash, unicum: Unicum): LfContractId.V1 =
    LfContractId.V1(discriminator, unicum.toContractIdSuffix(this))

  override final def compare(that: CantonContractIdVersion): Int = this.v.compare(that.v)
}

case object NonAuthenticatedContractId
    extends CantonContractIdVersion(NonNegativeInt.tryCreate(0)) {
  // The prefix for the suffix of non-authenticated (legacy) Canton contract IDs
  lazy val versionPrefixBytes: Bytes = Bytes.fromByteArray(Array(0xca.toByte, 0x00.toByte))

  val isAuthenticated: Boolean = false
}

case object AuthenticatedContractIdV1 extends CantonContractIdVersion(NonNegativeInt.tryCreate(1)) {
  // The prefix for the suffix of Canton contract IDs for contracts that can be authenticated (created in Protocol V4)
  lazy val versionPrefixBytes: Bytes = Bytes.fromByteArray(Array(0xca.toByte, 0x01.toByte))

  val isAuthenticated: Boolean = true
}

case object AuthenticatedContractIdV2 extends CantonContractIdVersion(NonNegativeInt.tryCreate(2)) {
  // The prefix for the suffix of Canton contract IDs for contracts that can be authenticated (created in Protocol V5)
  lazy val versionPrefixBytes: Bytes = Bytes.fromByteArray(Array(0xca.toByte, 0x02.toByte))

  val isAuthenticated: Boolean = true
}

case object AuthenticatedContractIdV3 extends CantonContractIdVersion(NonNegativeInt.tryCreate(3)) {
  // The prefix for the suffix of Canton contract IDs for contracts that can be authenticated (created in Protocol V6+)
  lazy val versionPrefixBytes: Bytes = Bytes.fromByteArray(Array(0xca.toByte, 0x03.toByte))

  val isAuthenticated: Boolean = true
}

object ContractIdSyntax {
  implicit class ScalaCodegenContractIdSyntax[T](contractId: ApiTypes.ContractId) {
    def toLf: LfContractId = LfContractId.assertFromString(contractId.toString)
  }

  implicit class JavaCodegenContractIdSyntax[T](contractId: ContractId[?]) {
    def toLf: LfContractId = LfContractId.assertFromString(contractId.contractId)
  }

  implicit class LfContractIdSyntax(private val contractId: LfContractId) extends AnyVal {
    def toProtoPrimitive: String = contractId.coid

    /** An [[LfContractId]] consists of
      * - a version (1 byte)
      * - a discriminator (32 bytes)
      * - a suffix (at most 94 bytes)
      * Thoses 1 + 32 + 94 = 127 bytes are base-16 encoded, so this makes 254 chars at most.
      * See https://github.com/digital-asset/daml/blob/main/daml-lf/spec/contract-id.rst
      */
    def toLengthLimitedString: String255 = checked(String255.tryCreate(contractId.coid))

    def encodeDeterministically: ByteString = ByteString.copyFromUtf8(toProtoPrimitive)

    /** Converts an [[LfContractId]] into a contract ID bound to a template usable with the Java codegen API.
      * `Unchecked` means that we do not check that the contract ID actually refers to a contract of
      * the template `T`.
      */
    def toContractIdUnchecked[T]: ContractId[T] =
      new ContractId(contractId.coid)
  }

  implicit val orderingLfContractId: Ordering[LfContractId] =
    Ordering.by[LfContractId, String](_.coid)
}

final case class MalformedContractId(id: String, message: String) {
  override def toString: String =
    s"malformed contract id '$id'" + (if (message.nonEmpty) s". $message" else "")
}
