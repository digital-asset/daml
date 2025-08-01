// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.ProtoDeserializationError.{
  ContractDeserializationError,
  UnknownContractAuthenticationDataVersion,
}
import com.digitalasset.canton.crypto.Salt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.v1
import com.digitalasset.canton.{admin, crypto}
import com.digitalasset.daml.lf.data.Bytes as LfBytes
import com.google.protobuf.ByteString
import io.scalaland.chimney.dsl.*

sealed trait ContractAuthenticationData extends PrettyPrinting {

  /** Defines the serialization of contract authentication data as stored inside
    * [[com.digitalasset.daml.lf.transaction.FatContractInstance.cantonData]]
    */
  def toLfBytes: LfBytes

  /** Defines the serialization of contract authentication data as stored inside a V30
    * [[SerializableContract]] serialization on the protocol API.
    */
  def toSerializableContractProtoV30: ByteString

  /** Defines the serialization of contract authentication data as stored inside a V30
    * [[SerializableContract]] serialization on the admin API.
    */
  def toSerializableContractAdminProtoV30: ByteString
}

/** Contract authentication data for contract IDs of version
  * [[com.digitalasset.daml.lf.value.Value.ContractId.V1]]
  */
final case class ContractAuthenticationDataV1(salt: Salt)(
    private val contractIdVersion: CantonContractIdV1Version
) extends ContractAuthenticationData {

  override protected def pretty: Pretty[ContractAuthenticationDataV1] = prettyOfClass(
    param("contract salt", _.salt.forHashing)
  )

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  override def toLfBytes: LfBytes =
    contractIdVersion match {
      case AuthenticatedContractIdVersionV10 | AuthenticatedContractIdVersionV11 =>
        LfBytes.fromByteArray(
          v1.UntypedVersionedMessage(
            v1.UntypedVersionedMessage.Wrapper.Data(
              v30.ContractAuthenticationData(Some(salt.toProtoV30)).toByteString
            ),
            30,
          ).toByteArray
        )
    }

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  override def toSerializableContractProtoV30: ByteString = salt.toProtoV30.toByteString

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  override def toSerializableContractAdminProtoV30: ByteString =
    salt.toProtoV30.transformInto[admin.crypto.v30.Salt].toByteString
}

// TODO(#23971) implement this stub
final case class ContractAuthenticationDataV2() extends ContractAuthenticationData {
  override def toLfBytes: LfBytes = ???

  override def toSerializableContractProtoV30: ByteString = ???

  override def toSerializableContractAdminProtoV30: ByteString = ???

  override protected def pretty: Pretty[ContractAuthenticationDataV2.this.type] = ???
}

object ContractAuthenticationData {

  private sealed trait ContractAuthenticationDataParser {
    protected def versionV1(
        version: CantonContractIdV1Version,
        bytes: ByteString,
    ): ParsingResult[ContractAuthenticationDataV1]

    protected def versionV2(
        version: CantonContractIdV2Version,
        bytes: ByteString,
    ): ParsingResult[ContractAuthenticationDataV2]

    // Helper method to turn the type member into a type variable that the compiler can reason about
    @inline
    private def parseInternal[CAD <: ContractAuthenticationData](
        version: CantonContractIdVersion { type AuthenticationData = CAD },
        bytes: ByteString,
    ): ParsingResult[CAD] =
      version match {
        // Pattern-matching on singletons in isolation is necessary for correct type inference
        case AuthenticatedContractIdVersionV10 =>
          versionV1(AuthenticatedContractIdVersionV10, bytes)
        case AuthenticatedContractIdVersionV11 =>
          versionV1(AuthenticatedContractIdVersionV11, bytes)
        case CantonContractIdV2Version0 =>
          Left(
            ContractDeserializationError(
              s"Unsupported contract ID version $version for authentication data"
            )
          )
      }

    def parse(
        version: CantonContractIdVersion,
        bytes: ByteString,
    ): ParsingResult[version.AuthenticationData] =
      parseInternal[version.AuthenticationData](version, bytes)
  }

  /** Parsing method for [[ContractAuthenticationData.toLfBytes]] */
  def fromLfBytes(
      contractIdVersion: CantonContractIdVersion,
      bytes: LfBytes,
  ): ParsingResult[contractIdVersion.AuthenticationData] =
    LfBytesContractAuthenticationDataParser.parse(contractIdVersion, bytes.toByteString)

  private object LfBytesContractAuthenticationDataParser extends ContractAuthenticationDataParser {
    override protected def versionV1(
        version: CantonContractIdV1Version,
        bytes: ByteString,
    ): ParsingResult[ContractAuthenticationDataV1] =
      for {
        proto <- ProtoConverter.protoParser(v1.UntypedVersionedMessage.parseFrom)(bytes)
        valueClass <- proto.version match {
          case 30 =>
            for {
              data <- ProtoConverter.protoParser(v30.ContractAuthenticationData.parseFrom)(
                proto.wrapper.data.getOrElse(ByteString.EMPTY)
              )
              v30.ContractAuthenticationData(saltP) = data
              salt <- ProtoConverter
                .required("salt", saltP)
                .flatMap(Salt.fromProtoV30)
            } yield ContractAuthenticationDataV1(salt)(version)
          case other => Left(UnknownContractAuthenticationDataVersion(other))
        }
      } yield valueClass

    override protected def versionV2(
        version: CantonContractIdV2Version,
        bytes: ByteString,
    ): ParsingResult[ContractAuthenticationDataV2] =
      Left(
        ContractDeserializationError(
          s"Unsupported contract ID version $version for authentication data"
        )
      )
  }

  /** Parsing method for [[ContractAuthenticationData.toSerializableContractProtoV30]] */
  def fromSerializableContractProtoV30(
      contractIdVersion: CantonContractIdVersion,
      authenticationDataP: ByteString,
  ): ParsingResult[ContractAuthenticationData] =
    SerializableContractAuthenticationDataParser.parse(contractIdVersion, authenticationDataP)

  private object SerializableContractAuthenticationDataParser
      extends ContractAuthenticationDataParser {
    override protected def versionV1(
        version: CantonContractIdV1Version,
        bytes: ByteString,
    ): ParsingResult[ContractAuthenticationDataV1] =
      for {
        salt <- ProtoConverter
          .protoParser(crypto.v30.Salt.parseFrom)(bytes)
          .flatMap(Salt.fromProtoV30)
      } yield ContractAuthenticationDataV1(salt)(version)

    override protected def versionV2(
        version: CantonContractIdV2Version,
        bytes: ByteString,
    ): ParsingResult[ContractAuthenticationDataV2] =
      Left(
        ContractDeserializationError(
          s"Unsupported contract ID version $version for authentication data"
        )
      )
  }

  /** Parsing method for [[ContractAuthenticationData.toSerializableContractAdminProtoV30]] */
  def fromSerializableContractAdminProtoV30(
      contractIdVersion: CantonContractIdVersion,
      authenticationDataP: ByteString,
  ): ParsingResult[contractIdVersion.AuthenticationData] =
    SerializableContractAdminContractAuthenticationDataParser.parse(
      contractIdVersion,
      authenticationDataP,
    )

  private object SerializableContractAdminContractAuthenticationDataParser
      extends ContractAuthenticationDataParser {
    override protected def versionV1(
        version: CantonContractIdV1Version,
        bytes: ByteString,
    ): ParsingResult[ContractAuthenticationDataV1] = for {
      adminSalt <- ProtoConverter.protoParser(admin.crypto.v30.Salt.parseFrom)(bytes)
      salt <- Salt.fromProtoV30(adminSalt.transformInto[crypto.v30.Salt])
    } yield ContractAuthenticationDataV1(salt)(version)

    override protected def versionV2(
        version: CantonContractIdV2Version,
        bytes: ByteString,
    ): ParsingResult[ContractAuthenticationDataV2] =
      Left(
        ContractDeserializationError(
          s"Unsupported contract ID version $version for authentication data"
        )
      )
  }
}
