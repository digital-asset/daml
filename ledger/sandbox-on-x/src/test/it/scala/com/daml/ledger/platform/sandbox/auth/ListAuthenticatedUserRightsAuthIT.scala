// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.user_management_service._
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import org.scalatest.Assertion

import java.util.UUID
import scala.concurrent.Future

class ListAuthenticatedUserRightsAuthIT extends ServiceCallAuthTests {
  private val testId = UUID.randomUUID().toString

  override def serviceCallName: String =
    "UserManagementService#ListUserRights(<authenticated-user>)"

  protected def serviceCall(context: ServiceCallContext): Future[Any] =
    stub(UserManagementServiceGrpc.stub(channel), context.token)
      .listUserRights(ListUserRightsRequest(identityProviderId = context.identityProviderId))

  protected def expectRights(
      context: ServiceCallContext,
      expectedRights: Vector[Right],
  ): Future[Assertion] =
    serviceCall(context).map(assertResult(ListUserRightsResponse(expectedRights))(_))

  private def getRights(context: ServiceCallContext, userId: String) =
    stub(UserManagementServiceGrpc.stub(channel), context.token)
      .listUserRights(
        ListUserRightsRequest(userId, identityProviderId = context.identityProviderId)
      )

  behavior of serviceCallName

  it should "deny unauthenticated access" taggedAs securityAsset.setAttack(
    attackUnauthenticated(threat = "Do not present a JWT")
  ) in {
    expectUnauthenticated(serviceCall(noToken))
  }

  it should "deny access for a standard token referring to an unknown user" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat = "Present a JWT with an unknown user")
    ) in {
    expectPermissionDenied(serviceCall(canReadAsUnknownUserStandardJWT))
  }

  it should "return rights of the 'participant_admin' when using its standard token" in {
    expectRights(
      canReadAsAdminStandardJWT,
      Vector(Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin()))),
    )
  }

  it should "return invalid argument for custom token" taggedAs securityAsset.setAttack(
    attackInvalidArgument(threat = "Present a custom admin JWT")
  ) in {
    expectInvalidArgument(serviceCall(canReadAsAdmin))
  }

  it should "allow access to a non-admin user's own rights" taggedAs securityAsset.setHappyCase(
    "Ledger API client can read non-admin user's own rights"
  ) in {
    val expectedRights = ListUserRightsResponse(Vector.empty)
    for {
      // admin creates user
      (alice, aliceContext) <- createUserByAdmin(testId + "-alice")
      // user accesses its own user record without specifying the id
      aliceRetrieved1 <- getRights(aliceContext, "")
      // user accesses its own user record with specifying the id
      aliceRetrieved2 <- getRights(aliceContext, alice.id)

    } yield assertResult((expectedRights, expectedRights))((aliceRetrieved1, aliceRetrieved2))
  }
}
