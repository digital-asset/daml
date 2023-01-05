// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.error.ErrorsAssertions
import com.daml.error.utils.ErrorDetails
import com.daml.ledger.api.v1.admin.user_management_service.Right
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, singleParticipant}
import com.daml.platform.sandbox.services.SubmitAndWaitDummyCommandHelpers
import com.google.protobuf.field_mask.FieldMask
import io.grpc.{Status, StatusRuntimeException}

import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success}

final class UpdateUserSelfDeactivationAuthIT
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

  it should "bar the user's self-deactivation" in {
    import com.daml.ledger.api.v1.admin.{user_management_service => proto}

    val userIdAlice = testId + "-alice-3"
    for {
      (_, alice0Context) <- createUserByAdmin(
        userId = userIdAlice,
        identityProviderId = "",
        rights = Vector(Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin()))),
      )
      err <- updateUser(
        accessToken = alice0Context.token.get,
        req = proto.UpdateUserRequest(
          user = Some(
            proto.User(
              id = userIdAlice,
              isDeactivated = true,
            )
          ),
          updateMask = Some(FieldMask(paths = scala.Seq("is_deactivated"))),
        ),
      ).transform {
        case Success(_) =>
          fail("Expected a failure when a user tries to self-deactivate, but received success")
        case Failure(e) => Success(e)
      }
    } yield {
      err match {
        case sre: StatusRuntimeException =>
          assertError(
            actual = sre,
            expectedStatusCode = Status.Code.INVALID_ARGUMENT,
            expectedMessage =
              "INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Requesting user cannot self-deactivate",
            expectedDetails = List(
              ErrorDetails.ErrorInfoDetail(
                "INVALID_ARGUMENT",
                Map(
                  "participantId" -> "'sandbox-participant'",
                  "category" -> "8",
                  "definite_answer" -> "false",
                ),
              )
            ),
            verifyEmptyStackTrace = false,
          )
        case _ => fail("Unexpected error", err)
      }
      assert(true)
    }

  }

}
