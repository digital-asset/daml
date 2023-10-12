// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.tracing.{TraceContext, Traced}

final case class TopologyStateForInitResponse(
    topologyTransactions: Traced[GenericStoredTopologyTransactionsX]
) {
  def toProtoV0: v0.TopologyStateForInitResponse =
    v0.TopologyStateForInitResponse(
      topologyTransactions = Some(topologyTransactions.value.toProtoV0)
    )
}

object TopologyStateForInitResponse {

  def fromProtoV0(responseP: v0.TopologyStateForInitResponse)(implicit
      traceContext: TraceContext
  ): ParsingResult[TopologyStateForInitResponse] = {
    val v0.TopologyStateForInitResponse(
      topologyTransactionsP
    ) = {
      responseP
    }
    for {
      topologyTransactions <- ProtoConverter.parseRequired(
        StoredTopologyTransactionsX.fromProtoV0,
        "topology_transactions",
        topologyTransactionsP,
      )
    } yield TopologyStateForInitResponse(
      Traced(topologyTransactions)(traceContext)
    )
  }
}
