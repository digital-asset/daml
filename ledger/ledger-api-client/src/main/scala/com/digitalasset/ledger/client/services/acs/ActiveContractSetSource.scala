// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.acs

import akka.stream.scaladsl.{Keep, Source}
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.grpc.adapter.client.akka.ClientAdapter
import com.digitalasset.ledger.api.v1.active_contracts_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.digitalasset.util.akkastreams.ExtractMaterializedValue
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

object ActiveContractSetSource {

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def apply(
      stub: (GetActiveContractsRequest, StreamObserver[GetActiveContractsResponse]) => Unit,
      request: GetActiveContractsRequest)(implicit esf: ExecutionSequencerFactory)
    : Source[GetActiveContractsResponse, Future[String]] = {

    ClientAdapter
      .serverStreaming(request, stub)
      .viaMat(new ExtractMaterializedValue(r => Some(r.offset)))(Keep.right)
  }
}
