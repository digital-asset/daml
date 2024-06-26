// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.data.Bytes as LfBytes
import com.digitalasset.canton.crypto.Salt
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}

import scala.util.chaining.*

final case class DriverContractMetadata(salt: Salt)
    extends HasVersionedWrapper[DriverContractMetadata]
    with PrettyPrinting {
  override protected def companionObj = DriverContractMetadata

  override def pretty: Pretty[DriverContractMetadata] = prettyOfClass(
    param("contract salt", _.salt.forHashing)
  )

  def toProtoV30: v30.DriverContractMetadata =
    v30.DriverContractMetadata(Some(salt.toProtoV30))

  def toLfBytes(protocolVersion: ProtocolVersion): LfBytes =
    toByteArray(protocolVersion).pipe(LfBytes.fromByteArray)
}

object DriverContractMetadata extends HasVersionedMessageCompanion[DriverContractMetadata] {
  override def name: String = "driver contract metadata"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v31,
      supportedProtoVersion(v30.DriverContractMetadata)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def fromProtoV30(
      driverContractMetadataP: v30.DriverContractMetadata
  ): ParsingResult[DriverContractMetadata] = {
    val v30.DriverContractMetadata(saltP) = driverContractMetadataP

    ProtoConverter
      .required("salt", saltP)
      .flatMap(Salt.fromProtoV30)
      .map(DriverContractMetadata(_))
  }
}
