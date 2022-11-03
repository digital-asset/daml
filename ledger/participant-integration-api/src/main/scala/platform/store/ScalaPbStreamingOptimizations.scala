// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import scalapb.GeneratedMessage
import scala.util.chaining._

object ScalaPbStreamingOptimizations {
  implicit class ScalaPbMessageWithPrecomputedSerializedSize[
      ScalaPbMsg <: GeneratedMessage with AnyRef
  ](scalaPbMsg: ScalaPbMsg) {

    /** Optimization for gRPC streams throughput.
      *
      * gRPC internal logic marshalls the protobuf response payloads sequentially before
      * sending them over the wire (see io.grpc.ServerCallImpl.sendMessageInternal), imposing as limit
      * the maximum marshalling throughput of a payload type.
      *
      * We've observed empirically that ScalaPB-generated message classes have associated marshallers
      * with significant latencies when encoding complex payloads (e.g. [[com.daml.ledger.api.v1.transaction_service.GetTransactionTreesResponse]]),
      * with the gRPC marshalling bottleneck appearing in some performance tests.
      *
      * As an alleviation of the problem, we can leverage the fact that ScalaPB message classes have the serializedSize value memoized,
      * (see [[scalapb.GeneratedMessage.writeTo]]), whose computation is roughly half of the entire marshalling step.
      *
      * This optimization method takes advantage of the memoized value and forces the message's serializedSize computation,
      * roughly doubling the maximum theoretical ScalaPB stream throughput over the gRPC server layer.
      *
      * @return A new message [[scalapb.GeneratedMessage]] with precomputed serializedSize.
      */
    def precomputeSerializedSize(): ScalaPbMsg =
      scalaPbMsg.tap(_.serializedSize)
  }
}
