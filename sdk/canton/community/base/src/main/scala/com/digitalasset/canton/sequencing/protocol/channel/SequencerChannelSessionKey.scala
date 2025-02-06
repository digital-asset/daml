// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol.channel

import com.digitalasset.canton.crypto.{AsymmetricEncrypted, v30 as crypto_v30}
import com.digitalasset.canton.protocol.ProtocolSymmetricKey
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

final case class SequencerChannelSessionKey(
    encryptedSessionKey: AsymmetricEncrypted[ProtocolSymmetricKey]
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SequencerChannelSessionKey.type
    ]
) extends HasProtocolVersionedWrapper[SequencerChannelSessionKey] {
  @transient override protected lazy val companionObj: SequencerChannelSessionKey.type =
    SequencerChannelSessionKey

  def toProtoV30: v30.SequencerChannelSessionKey =
    v30.SequencerChannelSessionKey(Some(encryptedSessionKey.toProtoV30))
}

object SequencerChannelSessionKey extends VersioningCompanion[SequencerChannelSessionKey] {
  override val name: String = "SequencerChannelSessionKey"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.dev)(
      v30.SequencerChannelSessionKey
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  def apply(
      encryptedSessionKey: AsymmetricEncrypted[ProtocolSymmetricKey],
      protocolVersion: ProtocolVersion,
  ): SequencerChannelSessionKey =
    SequencerChannelSessionKey(encryptedSessionKey)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def fromProtoV30(
      SequencerChannelSessionKeyP: v30.SequencerChannelSessionKey
  ): ParsingResult[SequencerChannelSessionKey] = {
    val v30.SequencerChannelSessionKey(encryptedSessionKeyP) =
      SequencerChannelSessionKeyP
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))

      encryptedSessionKey <- parseEncryptedSessionKey(encryptedSessionKeyP)

    } yield SequencerChannelSessionKey(encryptedSessionKey)(rpv)
  }

  def parseEncryptedSessionKey(
      encryptedSessionKeyP: Option[crypto_v30.AsymmetricEncrypted]
  ): ParsingResult[AsymmetricEncrypted[ProtocolSymmetricKey]] =
    ProtoConverter.parseRequired(
      AsymmetricEncrypted.fromProtoV30[ProtocolSymmetricKey],
      "encrypted_session_key",
      encryptedSessionKeyP,
    )
}
