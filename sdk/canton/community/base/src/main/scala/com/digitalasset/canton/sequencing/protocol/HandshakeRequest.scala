// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.version.ProtocolVersion

final case class HandshakeRequest(
    clientProtocolVersions: Seq[ProtocolVersion],
    minimumProtocolVersion: Option[ProtocolVersion],
) {

  // IMPORTANT: changing the version handshakes can lead to issues with upgrading synchronizers - be very careful
  // when changing the handshake message format
  def toProtoV30: v30.SequencerConnect.HandshakeRequest =
    v30.SequencerConnect.HandshakeRequest(
      clientProtocolVersions.map(_.toProtoPrimitive),
      minimumProtocolVersion.map(_.toProtoPrimitive),
    )
}
