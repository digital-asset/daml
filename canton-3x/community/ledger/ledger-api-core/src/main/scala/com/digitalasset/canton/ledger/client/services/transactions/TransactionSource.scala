// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.transactions

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsRequest,
  GetTransactionsResponse,
}
import com.digitalasset.canton.util.pekkostreams.ImmutableIterable
import io.grpc.stub.StreamObserver
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

object TransactionSource {

  def trees(
      stub: (GetTransactionsRequest, StreamObserver[GetTransactionTreesResponse]) => Unit,
      request: GetTransactionsRequest,
  )(implicit esf: ExecutionSequencerFactory): Source[TransactionTree, NotUsed] = {

    ClientAdapter
      .serverStreaming(request, stub)
      .mapConcat(batch => ImmutableIterable(batch.transactions))
  }

  def flat(
      stub: (GetTransactionsRequest, StreamObserver[GetTransactionsResponse]) => Unit,
      request: GetTransactionsRequest,
  )(implicit esf: ExecutionSequencerFactory): Source[Transaction, NotUsed] = {

    ClientAdapter
      .serverStreaming(request, stub)
      .mapConcat(batch => ImmutableIterable(batch.transactions))
  }
}
