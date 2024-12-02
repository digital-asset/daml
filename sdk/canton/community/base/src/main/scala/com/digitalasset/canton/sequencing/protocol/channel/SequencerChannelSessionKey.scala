// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol.channel

import com.digitalasset.canton.crypto.{AsymmetricEncrypted, SymmetricKey, v30 as crypto_v30}
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

final case class SequencerChannelSessionKey(
    encryptedSessionKey: AsymmetricEncrypted[SymmetricKey]
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

object SequencerChannelSessionKey
    extends HasProtocolVersionedCompanion[SequencerChannelSessionKey] {
  override val name: String = "SequencerChannelSessionKey"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.dev)(
      v30.SequencerChannelSessionKey
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  def apply(
      encryptedSessionKey: AsymmetricEncrypted[SymmetricKey],
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
  ): ParsingResult[AsymmetricEncrypted[SymmetricKey]] =
    ProtoConverter.parseRequired(
      AsymmetricEncrypted.fromProtoV30[SymmetricKey],
      "encrypted_session_key",
      encryptedSessionKeyP,
    )
}
