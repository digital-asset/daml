// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service.*
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthInterceptorSuppressionRule
import org.scalatest.Assertion

import java.util.UUID
import scala.concurrent.Future

class ListAuthenticatedUserRightsAuthIT extends ServiceCallAuthTests {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private val testId = UUID.randomUUID().toString

  override def serviceCallName: String =
    "UserManagementService#ListUserRights(<authenticated-user>)"

  protected def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(UserManagementServiceGrpc.stub(channel), context.token)
      .listUserRights(
        ListUserRightsRequest(
          userId = "",
          identityProviderId = context.identityProviderId,
        )
      )

  protected def expectRights(
      context: ServiceCallContext,
      expectedRights: Vector[Right],
  )(implicit env: TestConsoleEnvironment): Assertion = {
    import env.*
    serviceCall(context).map(assertResult(ListUserRightsResponse(expectedRights))(_)).futureValue
  }

  private def getRights(context: ServiceCallContext, userId: String) =
    stub(UserManagementServiceGrpc.stub(channel), context.token)
      .listUserRights(
        ListUserRightsRequest(userId, identityProviderId = context.identityProviderId)
      )

  serviceCallName should {

    "deny unauthenticated access" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat = "Do not present a JWT")
    ) in { implicit env =>
      import env.*
      expectUnauthenticated(serviceCall(noToken))
    }

    "deny access for a standard token referring to an unknown user" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present a JWT with an unknown user")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthInterceptorSuppressionRule) {
        expectPermissionDenied(serviceCall(canReadAsUnknownUserStandardJWT))
      }
    }

    "return rights of the 'participant_admin' when using its standard token" in { implicit env =>
      expectRights(
        canBeAnAdmin,
        Vector(Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin()))),
      )
    }

    "allow access to a non-admin user's own rights" taggedAs securityAsset.setHappyCase(
      "Ledger API client can read non-admin user's own rights"
    ) in { implicit env =>
      import env.*
      val expectedRights = ListUserRightsResponse(Vector.empty)
      val f = for {
        // admin creates user
        (alice, aliceContext) <- createUserByAdmin(testId + "-alice")
        // user accesses its own user record without specifying the id
        aliceRetrieved1 <- getRights(aliceContext, "")
        // user accesses its own user record with specifying the id
        aliceRetrieved2 <- getRights(aliceContext, alice.id)

      } yield assertResult((expectedRights, expectedRights))(
        (aliceRetrieved1, aliceRetrieved2)
      )
      f.futureValue
    }
  }
}
