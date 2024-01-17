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

  def toProtoV0: v0.DriverContractMetadata =
    v0.DriverContractMetadata(Some(salt.toProtoV0))

  def toLfBytes(protocolVersion: ProtocolVersion): LfBytes =
    toByteArray(protocolVersion).pipe(LfBytes.fromByteArray)
}

object DriverContractMetadata extends HasVersionedMessageCompanion[DriverContractMetadata] {
  override def name: String = "driver contract metadata"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v0.DriverContractMetadata)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def fromProtoV0(
      driverContractMetadataP: v0.DriverContractMetadata
  ): ParsingResult[DriverContractMetadata] = {
    val v0.DriverContractMetadata(saltP) = driverContractMetadataP

    ProtoConverter
      .required("salt", saltP)
      .flatMap(Salt.fromProtoV0)
      .map(DriverContractMetadata(_))
  }
}
