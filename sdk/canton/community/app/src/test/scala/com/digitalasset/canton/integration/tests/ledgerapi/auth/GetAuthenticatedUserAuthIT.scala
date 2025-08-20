// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.user_management_service.{
  GetUserRequest,
  GetUserResponse,
  User,
  UserManagementServiceGrpc,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthInterceptorSuppressionRule
import org.scalatest.Assertion

import java.util.UUID
import scala.concurrent.Future

/** Tests covering the special behaviour of GetUser wrt the authenticated user. */
class GetAuthenticatedUserAuthIT extends ServiceCallAuthTests {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private val testId = UUID.randomUUID().toString

  override def serviceCallName: String = "UserManagementService#GetUser(<authenticated-user>)"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(UserManagementServiceGrpc.stub(channel), context.token)
      .getUser(
        GetUserRequest(
          userId = "",
          identityProviderId = context.identityProviderId,
        )
      )

  private def expectUser(context: ServiceCallContext, expectedUser: User)(implicit
      env: TestConsoleEnvironment
  ): Assertion = {
    import env.*
    serviceCall(context).map(assertResult(GetUserResponse(Some(expectedUser)))(_)).futureValue
  }

  private def getUser(context: ServiceCallContext, userId: String) =
    stub(UserManagementServiceGrpc.stub(channel), context.token)
      .getUser(GetUserRequest(userId, identityProviderId = context.identityProviderId))

  serviceCallName should {

    "deny unauthenticated access" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat = "Do not present JWT")
    ) in { implicit env =>
      import env.*
      expectUnauthenticated(serviceCall(noToken))
    }

    "deny access for a standard token referring to an unknown user" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present JWT with an unknown user")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthInterceptorSuppressionRule) {
        expectPermissionDenied(serviceCall(canReadAsUnknownUserStandardJWT))
      }
    }

    "return the 'participant_admin' user when using its standard token" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a standard JWT"
      ) in { implicit env =>
      expectUser(
        canBeAnAdmin,
        User(
          id = "participant_admin",
          primaryParty = "",
          isDeactivated = false,
          metadata = Some(ObjectMeta("0", Map.empty)),
          identityProviderId = "",
        ),
      )
    }

    "allow access to a non-admin user's own user record" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can read non-admin user's own record"
      ) in { implicit env =>
      import env.*
      (for {
        // admin creates user
        (alice, aliceTokenContext) <- createUserByAdmin(testId + "-alice")
        // user accesses its own user record without specifying the id
        aliceRetrieved1 <- getUser(aliceTokenContext, "")
        // user accesses its own user record with specifying the id
        aliceRetrieved2 <- getUser(aliceTokenContext, alice.id)
        expected = GetUserResponse(Some(alice))
      } yield assertResult((expected, expected))((aliceRetrieved1, aliceRetrieved2))).futureValue
    }
  }
}
