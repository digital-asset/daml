// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.error.ErrorsAssertions
import com.daml.error.utils.ErrorDetails
import com.daml.ledger.api.v1.admin.user_management_service.{Right, UpdateUserRequest}
import com.daml.ledger.api.v1.admin.{user_management_service => user_management_service_proto}
import com.daml.ledger.api.v1.{admin => admin_proto}
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionsRequest,
  GetTransactionsResponse,
  TransactionServiceGrpc,
}
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, singleParticipant}
import com.daml.platform.sandbox.services.SubmitAndWaitDummyCommandHelpers
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import com.daml.timer.Delayed
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.google.protobuf.field_mask.FieldMask

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

final class OngoingStreamAuthIT
    extends ServiceCallAuthTests
    with SubmitAndWaitDummyCommandHelpers
    with ErrorsAssertions {

  private val UserManagementCacheExpiryInSeconds = 1

  override def config: Config = super.config.copy(
    participants = singleParticipant(
      ApiServerConfig.copy(
        userManagement = ApiServerConfig.userManagement
          .copy(
            cacheExpiryAfterWriteInSeconds = UserManagementCacheExpiryInSeconds
          )
      )
    )
  )

  override def serviceCallName: String = ""

  override protected def serviceCall(context: ServiceCallContext): Future[Any] = ???

  private val testId = UUID.randomUUID().toString
  val partyAlice = "alice-party"
  val partyAlice2 = "alice-party-2"

  protected override def prerequisiteParties: List[String] = List(partyAlice, partyAlice2)

  it should "abort an ongoing stream after user state has changed" taggedAs securityAsset.setAttack(
    streamAttack(threat = "Continue privileged stream access after revocation of rights")
  ) in {
    val userIdAlice = testId + "-alice"
    val receivedTransactionsCount = new AtomicInteger(0)
    val transactionStreamAbortedPromise = Promise[Throwable]()

    def observeTransactionsStream(
        token: Option[String],
        party: String,
    ): Unit = {
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
        begin = Option(ledgerBegin),
        end = None,
        filter = Some(
          new TransactionFilter(
            Map(party -> new Filters)
          )
        ),
      )
      val _ = stub(TransactionServiceGrpc.stub(channel), token)
        .getTransactions(request, observer)
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
      _ = observeTransactionsStream(tokenAlice, partyAlice)
      _ <- submitAndWaitF()
      // Making a change to the user Alice
      _ <- grantUserRightsByAdmin(
        userId = userIdAlice,
        Right(Right.Kind.CanActAs(Right.CanActAs(UUID.randomUUID().toString))),
      )
      _ <- Delayed.Future.by((UserManagementCacheExpiryInSeconds + 1).second)(
        Future(
          transactionStreamAbortedPromise.tryFailure(
            new AssertionError("Timed-out waiting while waiting for stream to abort")
          )
        )
      )
      t <- transactionStreamAbortedPromise.future
    } yield {
      t match {
        case sre: StatusRuntimeException =>
          assertError(
            actual = sre,
            expectedStatusCode = Status.Code.ABORTED,
            expectedMessage =
              "STALE_STREAM_AUTHORIZATION(2,0): Stale stream authorization. Retry quickly.",
            expectedDetails = List(
              ErrorDetails.ErrorInfoDetail(
                "STALE_STREAM_AUTHORIZATION",
                Map(
                  "participantId" -> "'sandbox-participant'",
                  "category" -> "2",
                  "definite_answer" -> "false",
                ),
              ),
              ErrorDetails.RetryInfoDetail(0.seconds),
            ),
            verifyEmptyStackTrace = false,
          )
        case _ => fail("Unexpected error", t)
      }
      assert(receivedTransactionsCount.get() >= 2)
    }
  }

  it should "abort an ongoing stream after user has been deactivated" taggedAs securityAsset
    .setAttack(
      streamAttack(threat = "Continue privileged stream access after user deactivation")
    ) in {
    val userIdAlice = testId + "-alice-2"
    val receivedTransactionsCount = new AtomicInteger(0)
    val transactionStreamAbortedPromise = Promise[Throwable]()

    def observeTransactionsStream(
        token: Option[String],
        party: String,
    ): Unit = {
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
        begin = Option(ledgerBegin),
        end = None,
        filter = Some(
          new TransactionFilter(
            Map(party -> new Filters)
          )
        ),
      )
      val _ = stub(TransactionServiceGrpc.stub(channel), token)
        .getTransactions(request, observer)
    }

    val canActAsAlice = Right(Right.Kind.CanActAs(Right.CanActAs(partyAlice2)))
    for {
      (userAlice, tokenAlice) <- createUserByAdmin(
        userId = userIdAlice,
        rights = Vector(canActAsAlice),
      )
      applicationId = userAlice.id
      submitAndWaitF = () =>
        submitAndWait(token = tokenAlice, party = partyAlice2, applicationId = applicationId)
      _ <- submitAndWaitF()
      _ = observeTransactionsStream(tokenAlice, partyAlice2)
      _ <- submitAndWaitF()
      // Deactivating Alice user:
      _ <- deactivateUserByAdmin(userId = userIdAlice)
      _ <- Delayed.Future.by((UserManagementCacheExpiryInSeconds + 1).second)(
        Future(
          transactionStreamAbortedPromise.tryFailure(
            new AssertionError("Timed-out waiting while waiting for stream to abort")
          )
        )
      )
      t <- transactionStreamAbortedPromise.future
    } yield {
      t match {
        case sre: StatusRuntimeException =>
          assertError(
            actual = sre,
            expectedStatusCode = Status.Code.ABORTED,
            expectedMessage =
              "STALE_STREAM_AUTHORIZATION(2,0): Stale stream authorization. Retry quickly.",
            expectedDetails = List(
              ErrorDetails.ErrorInfoDetail(
                "STALE_STREAM_AUTHORIZATION",
                Map(
                  "participantId" -> "'sandbox-participant'",
                  "category" -> "2",
                  "definite_answer" -> "false",
                ),
              ),
              ErrorDetails.RetryInfoDetail(0.seconds),
            ),
            verifyEmptyStackTrace = false,
          )
        case _ => fail("Unexpected error", t)
      }
      assert(receivedTransactionsCount.get() >= 2)
    }
  }

  private def deactivateUserByAdmin(userId: String): Future[Unit] = {
    stub(
      user_management_service_proto.UserManagementServiceGrpc.stub(channel),
      canReadAsAdminStandardJWT.token,
    )
      .updateUser(
        UpdateUserRequest(
          user = Some(
            user_management_service_proto.User(
              id = userId,
              isDeactivated = true,
              metadata = Some(admin_proto.object_meta.ObjectMeta()),
            )
          ),
          updateMask = Some(
            FieldMask(
              paths = Seq("is_deactivated")
            )
          ),
        )
      )
      .map(_ => ())
  }

  private def grantUserRightsByAdmin(
      userId: String,
      right: user_management_service_proto.Right,
  ): Future[Unit] = {
    val req = user_management_service_proto.GrantUserRightsRequest(userId, Seq(right))
    stub(
      user_management_service_proto.UserManagementServiceGrpc.stub(channel),
      canReadAsAdminStandardJWT.token,
    )
      .grantUserRights(req)
      .map(_ => ())
  }

}
