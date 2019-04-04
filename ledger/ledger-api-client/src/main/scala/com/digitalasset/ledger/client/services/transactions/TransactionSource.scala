// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.transactions

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.grpc.adapter.client.akka.ClientAdapter
import com.digitalasset.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsRequest,
  GetTransactionsResponse
}
import com.digitalasset.util.akkastreams.ImmutableIterable
import io.grpc.stub.StreamObserver

object TransactionSource {

  def trees(
      stub: (GetTransactionsRequest, StreamObserver[GetTransactionTreesResponse]) => Unit,
      request: GetTransactionsRequest)(
      implicit esf: ExecutionSequencerFactory): Source[TransactionTree, NotUsed] = {

    ClientAdapter
      .serverStreaming(request, stub)
      .mapConcat(batch => ImmutableIterable(batch.transactions))
  }

  def flat(
      stub: (GetTransactionsRequest, StreamObserver[GetTransactionsResponse]) => Unit,
      request: GetTransactionsRequest)(
      implicit esf: ExecutionSequencerFactory): Source[Transaction, NotUsed] = {

    ClientAdapter
      .serverStreaming(request, stub)
      .mapConcat(batch => ImmutableIterable(batch.transactions))
  }
}
