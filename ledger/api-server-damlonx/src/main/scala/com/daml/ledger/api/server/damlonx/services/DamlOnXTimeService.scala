// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v1.IndexService
import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.testing.time_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

class DamlOnXTimeService(indexService: IndexService)(
    implicit grpcExecutionContext: ExecutionContext,
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory)
    extends TimeServiceAkkaGrpc
    with AutoCloseable
    with GrpcApiService {

  override def close(): Unit = ()

  override protected def getTimeSource(
      request: GetTimeRequest): Source[GetTimeResponse, NotUsed] = {

    indexService.getLedgerRecordTimeStream
      .map { recordTime =>
        // TODO(JM): More direct timestamp conversion
        GetTimeResponse(Some(fromInstant(recordTime.toInstant)))
      }
  }

  override def setTime(request: SetTimeRequest): Future[Empty] =
    // FIXME(JM): Implement shim for setting time.
    // See issue https://github.com/digital-asset/daml/issues/347.
    ???

  override def bindService(): ServerServiceDefinition =
    TimeServiceGrpc.bindService(this, DirectExecutionContext)
}
