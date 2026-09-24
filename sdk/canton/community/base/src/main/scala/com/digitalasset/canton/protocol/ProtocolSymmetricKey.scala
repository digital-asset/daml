// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.crypto.{SymmetricKey, v30}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}

/** Wrapper for a symmetric key when tied to a particular protocol version */
final case class ProtocolSymmetricKey(key: SymmetricKey)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ProtocolSymmetricKey.type
    ]
) extends HasProtocolVersionedWrapper[ProtocolSymmetricKey] {

  def unwrap: SymmetricKey = key

  override protected val companionObj: ProtocolSymmetricKey.type = ProtocolSymmetricKey
}

object ProtocolSymmetricKey extends VersioningCompanion[ProtocolSymmetricKey] {
  override def name: String = "ProtocolSymmetricKey"

  override def versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.SymmetricKey)(
      supportedProtoVersion(_)(fromProtoV30),
      _.key.toProtoV30,
    )
  )

  private def fromProtoV30(
      keyP: v30.SymmetricKey
  ): ParsingResult[ProtocolSymmetricKey] = for {
    key <- SymmetricKey.fromProtoV30(keyP)
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
  } yield ProtocolSymmetricKey(key)(rpv)

  def apply(symmetricKey: SymmetricKey, protocolVersion: ProtocolVersion): ProtocolSymmetricKey =
    ProtocolSymmetricKey(symmetricKey)(
      ProtocolSymmetricKey.protocolVersionRepresentativeFor(protocolVersion)
    )

}
