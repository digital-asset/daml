// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data

import com.digitalasset.canton.crypto.HashBuilder
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  SupportedVersions,
  data,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}
import com.google.protobuf.ByteString

import java.time.Instant

final case class OrderingRequest(
    tag: String,
    payload: ByteString,
    orderingStartInstant: Option[Instant] =
      None, // Used for metrics support only, unset in unit and simulation tests
) {
  def addToHashBuilder(hashBuilder: HashBuilder): Unit =
    hashBuilder
      .add(payload)
      .add(tag)
      .add(orderingStartInstant.toString)
      .discard
}

final case class OrderingRequestBatchStats(requests: Int, bytes: Int)
object OrderingRequestBatchStats {
  val ForTesting: OrderingRequestBatchStats = OrderingRequestBatchStats(0, 0)
}

final case class OrderingRequestBatch private (
    requests: Seq[Traced[OrderingRequest]],
    epochNumber: EpochNumber,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      data.OrderingRequestBatch.type
    ]
) extends HasProtocolVersionedWrapper[OrderingRequestBatch] {

  def addToHashBuilder(hashBuilder: HashBuilder): Unit = {
    hashBuilder.add(epochNumber)
    requests.foreach { request =>
      hashBuilder.add(representativeProtocolVersion.representative.toProtoPrimitive)
      hashBuilder.add(request.traceContext.toString)
      request.value.addToHashBuilder(hashBuilder)
    }
  }

  lazy val stats: OrderingRequestBatchStats =
    OrderingRequestBatchStats(
      requests = requests.size,
      bytes = requests.map(_.value.payload.size).sum,
    )

  private def orderingRequestToProtoV30(
      orderingRequest: OrderingRequest,
      traceContext: Option[String],
  ): v30.OrderingRequest = v30.OrderingRequest.of(
    traceContext = traceContext.getOrElse(""),
    orderingRequest.tag,
    orderingRequest.payload,
    orderingRequest.orderingStartInstant.map(i =>
      com.google.protobuf.timestamp.Timestamp
        .of(i.getEpochSecond, i.getNano)
    ),
  )

  def toProtoV30: v30.Batch =
    v30.Batch.of(
      requests.map { orderingRequest =>
        orderingRequestToProtoV30(
          orderingRequest.value,
          orderingRequest.traceContext.asW3CTraceContext.map(_.parent),
        )
      },
      epochNumber,
    )

  override protected val companionObj: OrderingRequestBatch.type =
    OrderingRequestBatch
}

object OrderingRequestBatch extends VersioningCompanion[OrderingRequestBatch] {

  // TODO(#24736): make this possible to change
  val BatchValidityDurationEpochs = 500

  override def name: String = "OrderingRequestBatch"
  def create(
      requests: Seq[Traced[OrderingRequest]],
      epochNumber: EpochNumber,
  )(implicit synchronizerProtocolVersion: ProtocolVersion): OrderingRequestBatch =
    OrderingRequestBatch(
      requests,
      epochNumber,
    )(
      protocolVersionRepresentativeFor(synchronizerProtocolVersion)
    )

  def fromProtoV30(
      batch: v30.Batch
  ): ParsingResult[OrderingRequestBatch] =
    for {
      rpv <- protocolVersionRepresentativeFor(SupportedVersions.ProtoData)
    } yield OrderingRequestBatch(
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
      },
      EpochNumber(batch.epochNumber),
    )(rpv)

  override def versioningTable: framework.data.OrderingRequestBatch.VersioningTable =
    VersioningTable(
      SupportedVersions.ProtoData ->
        VersionedProtoCodec(
          SupportedVersions.CantonProtocol
        )(v30.Batch)(
          supportedProtoVersion(_)(
            fromProtoV30
          ),
          _.toProtoV30,
        )
    )
}
