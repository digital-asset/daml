// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.grpc

import com.daml.ledger.api.v1.TransactionServiceOuterClass

trait TransactionServiceAkkaGrpc
    extends TransactionServiceGrpc.TransactionService
    with AutoCloseable {
  protected implicit def esf: com.daml.grpc.adapter.ExecutionSequencerFactory
  protected implicit def mat: akka.stream.Materializer

  protected val killSwitch =
    akka.stream.KillSwitches.shared("TransactionServiceKillSwitch 23709913642917")
  protected val closed = new java.util.concurrent.atomic.AtomicBoolean(false)
  protected def closingError = com.daml.grpc.adapter.server.akka.ServerAdapter.closingError()
  def close(): Unit = {
    if (closed.compareAndSet(false, true)) killSwitch.abort(closingError)
  }

  def getTransactions(
      request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest,
      responseObserver: _root_.io.grpc.stub.StreamObserver[
        TransactionServiceOuterClass.GetTransactionsResponse
      ],
  ): Unit = {
    if (closed.get()) {
      responseObserver.onError(closingError)
    } else {
      val sink = com.daml.grpc.adapter.server.akka.ServerAdapter.toSink(responseObserver)
      getTransactionsSource(request).via(killSwitch.flow).runWith(sink)
      ()
    }
  }
  protected def getTransactionsSource(
      request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest
  ): akka.stream.scaladsl.Source[
    TransactionServiceOuterClass.GetTransactionsResponse,
    akka.NotUsed,
  ]

  def getTransactionTrees(
      request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest,
      responseObserver: _root_.io.grpc.stub.StreamObserver[
        TransactionServiceOuterClass.GetTransactionTreesResponse
      ],
  ): Unit = {
    if (closed.get()) {
      responseObserver.onError(closingError)
    } else {
      val sink = com.daml.grpc.adapter.server.akka.ServerAdapter.toSink(responseObserver)
      getTransactionTreesSource(request).via(killSwitch.flow).runWith(sink)
      ()
    }
  }
  protected def getTransactionTreesSource(
      request: com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest
  ): akka.stream.scaladsl.Source[
    TransactionServiceOuterClass.GetTransactionTreesResponse,
    akka.NotUsed,
  ]

}
