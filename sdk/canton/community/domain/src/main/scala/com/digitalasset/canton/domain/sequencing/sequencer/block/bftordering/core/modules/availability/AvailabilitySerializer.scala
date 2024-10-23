// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.Availability.RemoteProtocolMessage
import com.google.protobuf.ByteString

trait AvailabilitySerializer {
  def messageForTransport(message: RemoteProtocolMessage): (ByteString, Hash)
}

object AvailabilitySerializerImpl extends AvailabilitySerializer {
  override def messageForTransport(message: RemoteProtocolMessage): (ByteString, Hash) = {
    val bytes = message.getCryptographicEvidence
    val hashToSign = RemoteProtocolMessage.hashForSignature(message.from, bytes)

    bytes -> hashToSign
  }
}
