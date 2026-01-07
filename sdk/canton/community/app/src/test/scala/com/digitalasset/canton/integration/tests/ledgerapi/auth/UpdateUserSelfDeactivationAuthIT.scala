// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.ledger.api.v2.admin.user_management_service.Right
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.google.protobuf.field_mask.FieldMask
import io.grpc.{Status, StatusRuntimeException}

import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success}

final class UpdateUserSelfDeactivationAuthIT extends ServiceCallAuthTests with ErrorsAssertions {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "UserManagementService#UpdateUser(<self-deactivate>)"

  override protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

  private def updateUser(
      accessToken: String,
      req: proto.UpdateUserRequest,
  ): Future[proto.UpdateUserResponse] =
    stub(proto.UserManagementServiceGrpc.stub(channel), Some(accessToken))
      .updateUser(req)

  private def deleteUser(
      accessToken: String,
      req: proto.DeleteUserRequest,
  ): Future[proto.DeleteUserResponse] =
    stub(proto.UserManagementServiceGrpc.stub(channel), Some(accessToken))
      .deleteUser(req)

  private def revokeRights(
      accessToken: String,
      req: proto.RevokeUserRightsRequest,
  ): Future[proto.RevokeUserRightsResponse] =
    stub(proto.UserManagementServiceGrpc.stub(channel), Some(accessToken))
      .revokeUserRights(req)

  private def testSelfDeactivation[T](
      intent: String,
      apiCall: (String, String) => Future[T],
  )(implicit env: FixtureParam): Unit = {
    import env.*
    val userIdAlice = "alice-" + UUID.randomUUID().toString
    (for {
      (_, aliceContext) <- createUserByAdmin(
        userId = userIdAlice,
        rights = Vector(Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin()))),
      )
      err <- apiCall(aliceContext.token.value, userIdAlice)
        .transform {
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
              s"INVALID_ARGUMENT(8,${tid.take(8)}): The submitted request has invalid arguments: Requesting user cannot $intent",
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

  serviceCallName should {
    "ascertain that user cannot self-deactivate" in { implicit env =>
      testSelfDeactivation(
        "self-deactivate",
        (token, userId) =>
          updateUser(
            accessToken = token,
            req = proto.UpdateUserRequest(
              user = Some(
                proto.User(
                  id = userId,
                  primaryParty = "",
                  isDeactivated = true,
                  metadata = None,
                  identityProviderId = "",
                )
              ),
              updateMask = Some(FieldMask(paths = scala.Seq("is_deactivated"))),
            ),
          ),
      )
    }

    "ascertain that user cannot remove own admin rights" in { implicit env =>
      testSelfDeactivation(
        "remove own admin rights",
        (token, userId) =>
          revokeRights(
            accessToken = token,
            req = proto.RevokeUserRightsRequest(
              userId = userId,
              rights = Seq(Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin()))),
              identityProviderId = "",
            ),
          ),
      )
    }

    "ascertain that user cannot delete itself" in { implicit env =>
      testSelfDeactivation(
        "delete itself",
        (token, userId) =>
          deleteUser(
            accessToken = token,
            req = proto.DeleteUserRequest(
              userId = userId,
              identityProviderId = "",
            ),
          ),
      )
    }
  }

}
