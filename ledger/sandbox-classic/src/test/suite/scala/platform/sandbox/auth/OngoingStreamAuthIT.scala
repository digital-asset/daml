// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.util.concurrent.atomic.AtomicInteger
import java.util.{Timer, TimerTask, UUID}

import com.daml.error.ErrorsAssertions
import com.daml.error.utils.ErrorDetails
import com.daml.ledger.api.v1.admin.user_management_service.Right
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionsRequest,
  GetTransactionsResponse,
  TransactionServiceGrpc,
}
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.SubmitAndWaitDummyCommandHelpers
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import com.daml.ledger.api.v1.admin.{user_management_service => user_management_service_proto}

import scala.concurrent.{Future, Promise}

final class OngoingStreamAuthIT
    extends ServiceCallAuthTests
    with SubmitAndWaitDummyCommandHelpers
    with ErrorsAssertions {

  private val UserManagementCacheExpiryInSeconds = 1

  override protected def config: SandboxConfig = super.config.withUserManagementConfig(
    _.copy(cacheExpiryAfterWriteInSeconds = UserManagementCacheExpiryInSeconds)
  )

  override def serviceCallName: String = ""

  override protected def serviceCallWithToken(token: Option[String]): Future[Any] = ???

  private val testId = UUID.randomUUID().toString

  it should "abort an ongoing stream after user state has changed" in {
    val partyAlice = "alice-party"
    val userIdAlice = testId + "-alice"
    val receivedTransactionsCount = new AtomicInteger(0)
    val transactionStreamAbortedPromise = Promise[Throwable]()

    def observeTransactionsStream(
        token: Option[String],
        party: String,
    ): StreamObserver[GetTransactionsResponse] = {
      val observer = new StreamObserver[GetTransactionsResponse] {
        override def onNext(value: GetTransactionsResponse): Unit = {
          val _ = receivedTransactionsCount.incrementAndGet()
        }

        override def onError(t: Throwable): Unit = {
          val _ = transactionStreamAbortedPromise.trySuccess(t)
        }

        override def onCompleted(): Unit = ()
      }
      val request = new GetTransactionsRequest(
        ledgerId = unwrappedLedgerId,
        begin = Option(ledgerBegin),
        end = None,
        filter = Some(
          new TransactionFilter(
            Map(party -> new Filters)
          )
        ),
      )
      stub(TransactionServiceGrpc.stub(channel), token)
        .getTransactions(request, observer)
      observer
    }

    val canActAsAlice = Right(Right.Kind.CanActAs(Right.CanActAs(partyAlice)))
    for {
      (userAlice, tokenAlice) <- createUserByAdmin(
        userId = userIdAlice,
        rights = Vector(canActAsAlice),
      )
      applicationId = userAlice.id
      submitAndWaitF = () =>
        submitAndWait(token = tokenAlice, party = partyAlice, applicationId = applicationId)
      _ <- submitAndWaitF()
      streamObserver = observeTransactionsStream(tokenAlice, partyAlice)
      _ <- submitAndWaitF()
      // Making a change to the user Alice
      _ <- grantUserRightsByAdmin(
        userId = userIdAlice,
        Right(Right.Kind.CanActAs(Right.CanActAs(UUID.randomUUID().toString))),
      )
      _ = Thread.sleep(UserManagementCacheExpiryInSeconds.toLong * 1000)
      //
      _ <- submitAndWaitF()
      _ <- submitAndWaitF()
      _ <- submitAndWaitF()
      // Timer that finishes the stream in case it isn't aborted as expected
      timerTask = new TimerTask {
        override def run(): Unit = streamObserver.onError(
          new AssertionError("Timed-out waiting while waiting for stream to abort")
        )
      }
      _ = new Timer(true).schedule(timerTask, 100)
      t <- transactionStreamAbortedPromise.future
    } yield {
      timerTask.cancel()
      t match {
        case sre: StatusRuntimeException =>
          assertError(
            actual = sre,
            expectedCode = Status.Code.ABORTED,
            expectedMessage =
              "STALE_STREAM_CLAIMS(2,0): Authentication claims out of date. Retry quickly.",
            expectedDetails = List(
              ErrorDetails.ErrorInfoDetail(
                "STALE_STREAM_CLAIMS",
                Map(
                  "participantId" -> "'sandbox-participant'",
                  "category" -> "2",
                  "definite_answer" -> "false",
                ),
              ),
              ErrorDetails.RetryInfoDetail(retryDelayInSeconds = 1),
            ),
          )
        case _ => fail("Unexpected error", t)
      }
      assert(receivedTransactionsCount.get() >= 2)
    }
  }

  private def grantUserRightsByAdmin(
      userId: String,
      right: user_management_service_proto.Right,
  ): Future[Unit] = {
    val req = user_management_service_proto.GrantUserRightsRequest(userId, Seq(right))
    stub(
      user_management_service_proto.UserManagementServiceGrpc.stub(channel),
      canReadAsAdminStandardJWT,
    )
      .grantUserRights(req)
      .map(_ => ())
  }

}
