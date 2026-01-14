// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class TopologyStateForInitHashResponse(
    topologyStateHash: Hash
) {
  def toProtoV30: v30.DownloadTopologyStateForInitHashResponse =
    v30.DownloadTopologyStateForInitHashResponse(
      topologyStateHash = topologyStateHash.getCryptographicEvidence
    )
}

object TopologyStateForInitHashResponse {

  def fromProtoV30(
      responseP: v30.DownloadTopologyStateForInitHashResponse
  ): ParsingResult[TopologyStateForInitHashResponse] =
    for {
      hash <- ProtoConverter.parseRequired(
        Hash.fromProtoPrimitive,
        "hash",
        Some(responseP.topologyStateHash),
      )
    } yield TopologyStateForInitHashResponse(hash)
}
