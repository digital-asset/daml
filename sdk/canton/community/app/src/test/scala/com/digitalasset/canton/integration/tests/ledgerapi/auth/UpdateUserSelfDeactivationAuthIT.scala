// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service.Right
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.google.protobuf.field_mask.FieldMask
import io.grpc.{Status, StatusRuntimeException}

import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success}

final class UpdateUserSelfDeactivationAuthIT extends ServiceCallAuthTests with ErrorsAssertions {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "UserManagementService#UpdateUser(<self-deactivate>)"

  override protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

  private val testId = UUID.randomUUID().toString

  serviceCallName should {
    "bar the user's self-deactivation" in { implicit env =>
      import com.daml.ledger.api.v2.admin.user_management_service as proto
      import env.*

      val userIdAlice = testId + "-alice-3"
      (for {
        (_, alice0Context) <- createUserByAdmin(
          userId = userIdAlice,
          identityProviderId = "",
          rights = Vector(Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin()))),
        )
        err <- updateUser(
          accessToken = alice0Context.token.value,
          req = proto.UpdateUserRequest(
            user = Some(
              proto.User(
                id = userIdAlice,
                primaryParty = "",
                isDeactivated = true,
                metadata = None,
                identityProviderId = "",
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
              expectedMessage = tid =>
                s"INVALID_ARGUMENT(8,${tid.take(8)}): The submitted request has invalid arguments: Requesting user cannot self-deactivate",
              expectedDetails = tid =>
                submissionId =>
                  List(
                    ErrorDetails.ErrorInfoDetail(
                      "INVALID_ARGUMENT",
                      Map(
                        "participant" -> s"'${participant1.name}'",
                        "test" -> "'UpdateUserSelfDeactivationAuthIT'",
                        "category" -> "8",
                        "definite_answer" -> "false",
                        "submissionId" -> submissionId,
                        "tid" -> tid,
                      ),
                    ),
                    ErrorDetails.RequestInfoDetail(tid),
                  ),
            )
          case _ => fail("Unexpected error", err)
        }
        assert(true)
      }).futureValue
    }
  }

}
