// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin as admin_proto
import com.daml.ledger.api.v2.admin.user_management_service as user_management_service_proto
import com.daml.ledger.api.v2.admin.user_management_service.{Right, UpdateUserRequest}
import com.daml.ledger.api.v2.update_service.{
  GetUpdatesRequest,
  GetUpdatesResponse,
  UpdateServiceGrpc,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.timer.Delayed
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitAndWaitDummyCommandHelpers
import com.digitalasset.canton.integration.util.UpdateFormatHelpers.getUpdateFormat
import com.digitalasset.canton.topology.PartyId
import com.google.protobuf.field_mask.FieldMask
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}

final class OngoingStreamAuthIT
    extends ServiceCallAuthTests
    with SubmitAndWaitDummyCommandHelpers
    with ErrorsAssertions {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "OngoingStreamAuthorizer"

  override protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

  private val testId = UUID.randomUUID().toString
  val partyAlice = "alice-party-1"
  val partyAlice2 = "alice-party-2"

  protected override def prerequisiteParties: List[String] = List(partyAlice, partyAlice2)

  serviceCallName should {
    "abort an ongoing stream after user state has changed" taggedAs securityAsset.setAttack(
      streamAttack(threat = "Continue privileged stream access after revocation of rights")
    ) in { implicit env =>
      import env.*
      val userIdAlice = testId + "-alice"
      val receivedTransactionsCount = new AtomicInteger(0)
      val transactionStreamAbortedPromise = Promise[Throwable]()

      def observeTransactionsStream(
          token: Option[String],
          party: PartyId,
      ): Unit = {
        val observer = new StreamObserver[GetUpdatesResponse] {
          override def onNext(value: GetUpdatesResponse): Unit = {
            val _ = receivedTransactionsCount.incrementAndGet()
          }

          override def onError(t: Throwable): Unit = {
            val _ = transactionStreamAbortedPromise.trySuccess(t)
          }

          override def onCompleted(): Unit = ()
        }
        val request = new GetUpdatesRequest(
          beginExclusive = participantBegin,
          endInclusive = None,
          filter = None,
          verbose = false,
          updateFormat = Some(getUpdateFormat(Set(party))),
        )
        val _ = stub(UpdateServiceGrpc.stub(channel), token)
          .getUpdates(request, observer)
      }

      val partyAliceId = env.participant1.parties.find(partyAlice)

      val canActAsAlice = Right(Right.Kind.CanActAs(Right.CanActAs(partyAliceId.toProtoPrimitive)))
      val f = for {
        (userAlice, aliceContext) <- createUserByAdmin(
          userId = userIdAlice,
          rights = Vector(canActAsAlice),
        )
        userId = userAlice.id
        submitAndWaitF = () =>
          submitAndWait(
            token = aliceContext.token,
            party = partyAliceId.toProtoPrimitive,
            userId = userId,
          )
        _ <- submitAndWaitF()
        _ = observeTransactionsStream(aliceContext.token, partyAliceId)
        _ <- submitAndWaitF()
        // Making a change to the user Alice
        _ <- grantUserRightsByAdmin(
          userId = userIdAlice,
          Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin())),
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
                    "participant" -> s"${participant1.name}",
                    "test" -> s"${this.getClass.getSimpleName}",
                    "category" -> "2",
                    "definite_answer" -> "false",
                  ),
                ),
                ErrorDetails.RetryInfoDetail(0.nanoseconds),
              ),
              verifyEmptyStackTrace = false,
            )
          case _ => fail("Unexpected error", t)
        }
        assert(receivedTransactionsCount.get() >= 2)
      }
      f.futureValue
    }

    "abort an ongoing stream after user has been deactivated" taggedAs securityAsset
      .setAttack(
        streamAttack(threat = "Continue privileged stream access after user deactivation")
      ) in { implicit env =>
      import env.*
      val userIdAlice = testId + "-alice-2"
      val receivedTransactionsCount = new AtomicInteger(0)
      val transactionStreamAbortedPromise = Promise[Throwable]()

      def observeTransactionsStream(
          token: Option[String],
          party: PartyId,
      ): Unit = {
        val observer = new StreamObserver[GetUpdatesResponse] {
          override def onNext(value: GetUpdatesResponse): Unit = {
            val _ = receivedTransactionsCount.incrementAndGet()
          }

          override def onError(t: Throwable): Unit = {
            val _ = transactionStreamAbortedPromise.trySuccess(t)
          }

          override def onCompleted(): Unit = ()
        }
        val request = new GetUpdatesRequest(
          beginExclusive = participantBegin,
          endInclusive = None,
          filter = None,
          verbose = false,
          updateFormat = Some(getUpdateFormat(Set(party))),
        )
        val _ = stub(UpdateServiceGrpc.stub(channel), token)
          .getUpdates(request, observer)
      }

      val partyAlice2Id = env.participant1.parties.find(partyAlice2)

      val canActAsAlice = Right(Right.Kind.CanActAs(Right.CanActAs(partyAlice2Id.toProtoPrimitive)))
      val f = for {
        (userAlice, aliceContext) <- createUserByAdmin(
          userId = userIdAlice,
          rights = Vector(canActAsAlice),
        )
        userId = userAlice.id
        submitAndWaitF = () =>
          submitAndWait(
            token = aliceContext.token,
            party = partyAlice2Id.toProtoPrimitive,
            userId = userId,
          )
        _ <- submitAndWaitF()
        _ = observeTransactionsStream(aliceContext.token, partyAlice2Id)
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
                    "participant" -> s"${participant1.name}",
                    "test" -> s"${this.getClass.getSimpleName}",
                    "category" -> "2",
                    "definite_answer" -> "false",
                  ),
                ),
                ErrorDetails.RetryInfoDetail(0.nanoseconds),
              ),
              verifyEmptyStackTrace = false,
            )
          case _ => fail("Unexpected error", t)
        }
        assert(receivedTransactionsCount.get() >= 2)
      }
      f.futureValue
    }
  }

  private def deactivateUserByAdmin(userId: String)(implicit ec: ExecutionContext): Future[Unit] =
    stub(
      user_management_service_proto.UserManagementServiceGrpc.stub(channel),
      canBeAnAdmin.token,
    )
      .updateUser(
        UpdateUserRequest(
          user = Some(
            user_management_service_proto.User(
              id = userId,
              primaryParty = "",
              isDeactivated = true,
              metadata = Some(admin_proto.object_meta.ObjectMeta.defaultInstance),
              identityProviderId = "",
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

  private def grantUserRightsByAdmin(
      userId: String,
      right: user_management_service_proto.Right,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val req = user_management_service_proto.GrantUserRightsRequest(
      userId = userId,
      rights = Seq(right),
      identityProviderId = "",
    )
    stub(
      user_management_service_proto.UserManagementServiceGrpc.stub(channel),
      canBeAnAdmin.token,
    )
      .grantUserRights(req)
      .map(_ => ())
  }

}
