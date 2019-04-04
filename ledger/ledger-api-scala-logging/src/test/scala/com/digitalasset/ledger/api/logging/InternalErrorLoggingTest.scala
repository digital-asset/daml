// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.logging
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import com.digitalasset.ledger.api.logging.FailingServerFixture.Exceptions
import com.digitalasset.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionEndRequest,
  CompletionStreamRequest,
  CompletionStreamResponse
}
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc
}
import com.digitalasset.ledger.api.v1.package_service.{GetPackageRequest, PackageServiceGrpc}
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionTreesResponse,
  GetTransactionsRequest,
  GetTransactionsResponse,
  TransactionServiceGrpc
}
import io.grpc.stub.StreamObserver
import org.scalatest.{Assertion, AsyncWordSpec, BeforeAndAfterEach, OptionValues}

import scala.concurrent.{Future, Promise}
import scala.util.Success

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class InternalErrorLoggingTest
    extends AsyncWordSpec
    with FailingServerFixture
    with TestAppenderFixture
    with OptionValues
    with BeforeAndAfterEach {

  import Exceptions._

  private implicit class FutureOps[T](val f: Future[T]) {

    def getLogs: Future[Vector[ILoggingEvent]] =
      f.transform(t => Success(TestAppender.flushLoggingEvents))
  }

  "Error logging" should {
    "happen" when {
      "unexpected exception is thrown" in {
        LedgerIdentityServiceGrpc
          .stub(getChannel)
          .getLedgerIdentity(GetLedgerIdentityRequest())
          .getLogs
          .map(firstErrorIsOfClass(UnknownException.getClass))
      }
      "unexpected exception is returned in future" in {
        completionStub
          .completionEnd(CompletionEndRequest())
          .getLogs
          .map(firstErrorIsOfClass(UnknownException.getClass))
      }
      "unexpected exception is passed to response observer" in {

        val p = Promise[Vector[ILoggingEvent]]()

        completionStub
          .completionStream(
            CompletionStreamRequest(),
            new LogFlushingObserver[CompletionStreamResponse](p)
          )
        p.future.map(firstErrorIsOfClass(UnknownException.getClass))
      }
      "internal grpc exception is thrown" in {
        transactionStub
          .getTransactionByEventId(GetTransactionByEventIdRequest())
          .getLogs
          .map(firstErrorIsOfClass(InternalGrpc.getClass))
      }
      "internal grpc exception is returned in future" in {
        PackageServiceGrpc
          .stub(getChannel)
          .getPackage(GetPackageRequest())
          .getLogs
          .map(firstErrorIsOfClass(InternalGrpc.getClass))
      }
      "internal grpc exception is passed to response observer" in {

        val p = Promise[Vector[ILoggingEvent]]()

        transactionStub
          .getTransactionTrees(
            GetTransactionsRequest(),
            new LogFlushingObserver[GetTransactionTreesResponse](p)
          )
        p.future.map(firstErrorIsOfClass(InternalGrpc.getClass))
      }
    }

    "not happen" when {
      "non-internal grpc exception is thrown" in {
        transactionStub
          .getLedgerEnd(GetLedgerEndRequest())
          .getLogs
          .map(_ shouldBe empty)
      }

      "non-internal grpc exception is returned in future" in {
        transactionStub
          .getTransactionById(GetTransactionByIdRequest())
          .getLogs
          .map(_ shouldBe empty)
      }

      "non-internal grpc exception is passed to response observer" in {

        val p = Promise[Vector[ILoggingEvent]]()

        transactionStub
          .getTransactions(
            GetTransactionsRequest(),
            new LogFlushingObserver[GetTransactionsResponse](p)
          )

        p.future.map(_ shouldBe empty)
      }
    }
  }
  private def transactionStub = {
    TransactionServiceGrpc
      .stub(getChannel)
  }
  private def completionStub = {
    CommandCompletionServiceGrpc
      .stub(getChannel)
  }
  private def firstErrorIsOfClass(errorClass: Class[_])(
      events: Vector[ILoggingEvent]): Assertion = {
    firstError(events).getThrowableProxy.getClassName shouldEqual errorClass.getName
  }
  private def firstError(events: Vector[ILoggingEvent]): ILoggingEvent = {
    events.find(_.getLevel == Level.ERROR).value
  }

  class LogFlushingObserver[T](p: Promise[Vector[ILoggingEvent]]) extends StreamObserver[T] {
    override def onNext(v: T): Unit =
      p.failure(new IllegalStateException("unexpected elem"))
    override def onError(throwable: Throwable): Unit =
      p.success(TestAppender.flushLoggingEvents)
    override def onCompleted(): Unit =
      p.failure(new IllegalStateException("unexpected completion"))
  }
}
