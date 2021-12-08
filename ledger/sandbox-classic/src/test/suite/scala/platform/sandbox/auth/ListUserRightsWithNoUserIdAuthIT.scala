// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.user_management_service._
import org.scalatest.Assertion

import scala.concurrent.Future

class ListUserRightsWithNoUserIdAuthIT extends ServiceCallAuthTests {
  override def serviceCallName: String = "UserManagementService#ListUserRights(<no-user-id>)"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(UserManagementServiceGrpc.stub(channel), token).listUserRights(ListUserRightsRequest())

  protected def expectRights(
      token: Option[String],
      expectedRights: Vector[Right],
  ): Future[Assertion] =
    serviceCallWithToken(token).map(assertResult(ListUserRightsResponse(expectedRights))(_))

  behavior of serviceCallName

  it should "deny unauthenticated access" in {
    expectUnauthenticated(serviceCallWithToken(None))
  }

  it should "deny access for a standard token referring to an unknown user" in {
    expectUnauthenticated(serviceCallWithToken(canReadAsUnknownUserStandardJWT))
  }

  it should "return rights of the 'participant_admin' when using its standard token" in {
    expectRights(
      canReadAsAdminStandardJWT,
      Vector(Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin()))),
    )
  }

  it should "return rights corresponding ot the decoded custom token for an admin" in {
    expectRights(
      canReadAsAdmin,
      Vector(Right(Right.Kind.ParticipantAdmin(Right.ParticipantAdmin()))),
    )
  }

  it should "return the rights corresponding to the decoded custom token for a random user with actAs rights" in {
    expectRights(
      randomUserCanActAsRandomParty,
      Vector(Right(Right.Kind.CanActAs(Right.CanActAs(randomParty)))),
    )
  }

  it should "return the rights corresponding to the decoded custom token for a random user with readAs rights" in {
    expectRights(
      randomUserCanReadAsRandomParty,
      Vector(Right(Right.Kind.CanReadAs(Right.CanReadAs(randomParty)))),
    )
  }

  it should "return the rights corresponding to the decoded custom token with no user and actAs rights" in {
    expectRights(
      canActAsRandomParty,
      Vector(Right(Right.Kind.CanActAs(Right.CanActAs(randomParty)))),
    )
  }

  it should "return the rights corresponding to the decoded custom token with no user and readAs rights" in {
    expectRights(
      canReadAsRandomParty,
      Vector(Right(Right.Kind.CanReadAs(Right.CanReadAs(randomParty)))),
    )
  }

}
