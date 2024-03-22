// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.tracing.{TraceContext, Traced}

final case class TopologyStateForInitResponse(
    topologyTransactions: Traced[GenericStoredTopologyTransactionsX]
) {
  def toProtoV30: v30.DownloadTopologyStateForInitResponse =
    v30.DownloadTopologyStateForInitResponse(
      topologyTransactions = Some(topologyTransactions.value.toProtoV30)
    )
}

object TopologyStateForInitResponse {

  def fromProtoV30(responseP: v30.DownloadTopologyStateForInitResponse)(implicit
      traceContext: TraceContext
  ): ParsingResult[TopologyStateForInitResponse] = {
    val v30.DownloadTopologyStateForInitResponse(
      topologyTransactionsP
    ) = {
      responseP
    }
    for {
      topologyTransactions <- ProtoConverter.parseRequired(
        StoredTopologyTransactionsX.fromProtoV30,
        "topology_transactions",
        topologyTransactionsP,
      )
    } yield TopologyStateForInitResponse(
      Traced(topologyTransactions)(traceContext)
    )
  }
}
