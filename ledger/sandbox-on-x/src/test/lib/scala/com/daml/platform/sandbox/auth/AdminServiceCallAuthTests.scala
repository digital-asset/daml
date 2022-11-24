// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

import java.util.UUID
import scala.concurrent.Future

//TODO DPP-1299 Include tests for IdentityProviderAdmin
trait AdminServiceCallAuthTests extends SecuredServiceCallAuthTests {

  private val signedIncorrectly = Option(toHeader(adminToken, UUID.randomUUID.toString))

  protected def serviceCallWithFreshUser(rights: Vector[proto.Right.Kind]): Future[Any] =
    createUserByAdmin(UUID.randomUUID().toString, rights.map(proto.Right(_)))
      .flatMap { case (_, token) => serviceCallWithToken(token) }

  it should "deny calls with an invalid signature" taggedAs adminSecurityAsset.setAttack(
    attackUnauthenticated(threat = "Present an admin JWT signed by unknown key")
  ) in {
    expectUnauthenticated(serviceCallWithToken(signedIncorrectly))
  }
  it should "deny calls with an expired admin token" taggedAs adminSecurityAsset.setAttack(
    attackUnauthenticated(threat = "Present an expired admin JWT")
  ) in {
    expectUnauthenticated(serviceCallWithToken(canReadAsAdminExpired))
  }
  it should "deny calls with a read-only token" taggedAs adminSecurityAsset.setAttack(
    attackPermissionDenied(threat = "Present a read-only user JWT with an unknown party")
  ) in {
    expectPermissionDenied(serviceCallWithToken(canReadAsRandomParty))
  }
  it should "deny calls with a read/write token" taggedAs adminSecurityAsset.setAttack(
    attackPermissionDenied(threat = "Present a read/write user JWT for an unknown party")
  ) in {
    expectPermissionDenied(serviceCallWithToken(canActAsRandomParty))
  }
  it should "allow calls with explicitly non-expired admin token" taggedAs adminSecurityAsset
    .setHappyCase(
      "Ledger API client can make a call with explicitly non-expired admin JWT"
    ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdminExpiresTomorrow))
  }
  it should "allow calls with admin token without expiration" taggedAs adminSecurityAsset
    .setHappyCase(
      "Ledger API client can make a call with admin JWT without expiration"
    ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdmin))
  }
  it should "allow calls for 'participant_admin' user without expiration" taggedAs adminSecurityAsset
    .setHappyCase(
      "Ledger API client can make a call with 'participant_admin' JWT without expiration"
    ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdminStandardJWT))
  }
  it should "allow calls with freshly created admin user" taggedAs adminSecurityAsset.setHappyCase(
    "Ledger API client can make a call with freshly created admin user"
  ) in {
    expectSuccess(
      serviceCallWithFreshUser(
        Vector(proto.Right.Kind.ParticipantAdmin(proto.Right.ParticipantAdmin()))
      )
    )
  }
  it should "deny calls with freshly created non-admin user" taggedAs adminSecurityAsset.setAttack(
    attackPermissionDenied(threat = "Present a user JWT for a freshly created non-admin user")
  ) in {
    expectPermissionDenied(serviceCallWithFreshUser(Vector.empty))
  }
  it should "deny calls with user token for 'unknown_user' without expiration" taggedAs adminSecurityAsset
    .setAttack(
      attackPermissionDenied(threat = "Present a user JWT for 'unknown_user' without expiration")
    ) in {
    expectPermissionDenied(serviceCallWithToken(canReadAsUnknownUserStandardJWT))
  }
  it should "deny calls with user token for '!!invalid_user!!' without expiration" taggedAs adminSecurityAsset
    .setAttack(
      attackInvalidArgument(threat =
        "Present a JWT with unparseable '!!invalid_user!!' without expiration"
      )
    ) in {
    expectInvalidArgument(serviceCallWithToken(canReadAsInvalidUserStandardJWT))
  }
  it should "allow calls with the correct ledger ID" taggedAs adminSecurityAsset.setHappyCase(
    "Ledger API client can make a call with the known ledger ID"
  ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdminActualLedgerId))
  }
  it should "deny calls with a random ledger ID" taggedAs adminSecurityAsset.setAttack(
    attackPermissionDenied(threat = "Present a JWT with an unknown ledger ID")
  ) in {
    expectPermissionDenied(serviceCallWithToken(canReadAsAdminRandomLedgerId))
  }
  it should "allow calls with the correct participant ID" taggedAs adminSecurityAsset.setHappyCase(
    "Ledger API client can make a call with the known participant ID"
  ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdminActualParticipantId))
  }
  it should "deny calls with a unknown participant ID" taggedAs adminSecurityAsset.setAttack(
    attackPermissionDenied(threat = "Present an admin JWT with an unknown participant ID")
  ) in {
    expectPermissionDenied(serviceCallWithToken(canReadAsAdminRandomParticipantId))
  }
}
