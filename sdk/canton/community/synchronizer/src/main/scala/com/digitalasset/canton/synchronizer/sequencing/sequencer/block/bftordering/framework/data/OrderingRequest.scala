// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data

import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1.{
  Batch as ProtoBatch,
  OrderingRequest as ProtoOrderingRequest,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString

import java.time.Instant

final case class OrderingRequest(
    tag: String,
    payload: ByteString,
    orderingStartInstant: Option[Instant] =
      None, // Used for metrics support only, unset in unit and simulation tests
) {

  def toProto(traceContext: Option[String]): ProtoOrderingRequest = ProtoOrderingRequest.of(
    traceContext = traceContext.getOrElse(""),
    tag,
    payload,
    orderingStartInstant.map(i =>
      com.google.protobuf.timestamp.Timestamp
        .of(i.getEpochSecond, i.getNano)
    ),
  )
}

final case class OrderingRequestBatchStats(requests: Int, bytes: Int)
object OrderingRequestBatchStats {
  val ForTesting: OrderingRequestBatchStats = OrderingRequestBatchStats(0, 0)
}

final case class OrderingRequestBatch(requests: Seq[Traced[OrderingRequest]]) {

  lazy val stats: OrderingRequestBatchStats =
    OrderingRequestBatchStats(
      requests = requests.size,
      bytes = requests.map(_.value.payload.size).sum,
    )

  def toProto: ProtoBatch =
    ProtoBatch.of(requests.map { orderingRequest =>
      orderingRequest.value.toProto(orderingRequest.traceContext.asW3CTraceContext.map(_.parent))
    })
}

object OrderingRequestBatch {
  def fromProto(
      value: Option[ProtoBatch]
  ): ParsingResult[OrderingRequestBatch] =
    Right(
      OrderingRequestBatch(value match {
        case Some(batch) =>
          batch.orderingRequests.map { protoOrderingRequest =>
            Traced.fromPair[OrderingRequest](
              (
                OrderingRequest(
                  protoOrderingRequest.tag,
                  protoOrderingRequest.payload,
                  protoOrderingRequest.orderingStartInstant.map(i =>
                    Instant.ofEpochSecond(i.seconds, i.nanos.toLong)
                  ),
                ),
                TraceContext.fromW3CTraceParent(protoOrderingRequest.traceContext),
              )
            )
          }
        case None => Seq.empty
      })
    )
}
