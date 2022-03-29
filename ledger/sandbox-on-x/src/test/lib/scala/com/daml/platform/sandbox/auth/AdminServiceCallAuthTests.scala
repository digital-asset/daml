// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

import java.util.UUID
import scala.concurrent.Future

trait AdminServiceCallAuthTests extends SecuredServiceCallAuthTests {

  private val signedIncorrectly = Option(toHeader(adminToken, UUID.randomUUID.toString))

  protected def serviceCallWithFreshUser(rights: Vector[proto.Right.Kind]): Future[Any] =
    createUserByAdmin(UUID.randomUUID().toString, rights.map(proto.Right(_)))
      .flatMap { case (_, token) => serviceCallWithToken(token) }

  it should "deny calls with an invalid signature" taggedAs securityAsset.setAttack(
    attack(threat = "Exploit a token with an invalid signature")
  ) in {
    expectUnauthenticated(serviceCallWithToken(signedIncorrectly))
  }
  it should "deny calls with an expired admin token" taggedAs securityAsset.setAttack(
    attack(threat = "Exploit an expired admin token")
  ) in {
    expectUnauthenticated(serviceCallWithToken(canReadAsAdminExpired))
  }
  it should "deny calls with a read-only token" taggedAs securityAsset.setAttack(
    attack(threat = "Exploit a read-only token for a random party")
  ) in {
    expectPermissionDenied(serviceCallWithToken(canReadAsRandomParty))
  }
  it should "deny calls with a read/write token" taggedAs securityAsset.setAttack(
    attack(threat = "Exploit read/write token for a random party")
  ) in {
    expectPermissionDenied(serviceCallWithToken(canActAsRandomParty))
  }
  it should "allow calls with explicitly non-expired admin token" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can connect with explicitly non-expired admin token"
    ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdminExpiresTomorrow))
  }
  it should "allow calls with admin token without expiration" taggedAs securityAsset.setHappyCase(
    "Ledger API client can connect with admin token without expiration"
  ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdmin))
  }
  it should "allow calls with user token for 'participant_admin' without expiration" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can connect with user token for 'participant_admin' without expiration"
    ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdminStandardJWT))
  }
  it should "allow calls with freshly created admin user" taggedAs securityAsset.setHappyCase(
    "Ledger API client can connect with freshly created admin user"
  ) in {
    expectSuccess(
      serviceCallWithFreshUser(
        Vector(proto.Right.Kind.ParticipantAdmin(proto.Right.ParticipantAdmin()))
      )
    )
  }
  it should "deny calls with freshly created non-admin user" taggedAs securityAsset.setAttack(
    attack(threat = "Exploit a freshly created non-admin user")
  ) in {
    expectPermissionDenied(serviceCallWithFreshUser(Vector.empty))
  }
  it should "deny calls with user token for 'unknown_user' without expiration" taggedAs securityAsset
    .setAttack(
      attack(threat = "Exploit a user token for 'unknown_user' without expiration")
    ) in {
    expectPermissionDenied(serviceCallWithToken(canReadAsUnknownUserStandardJWT))
  }
  it should "deny calls with user token for '!!invalid_user!!' without expiration" taggedAs securityAsset
    .setAttack(
      attack(threat = "Exploit a token for '!!invalid_user!!' without expiration")
    ) in {
    expectInvalidArgument(serviceCallWithToken(canReadAsInvalidUserStandardJWT))
  }
  it should "allow calls with the correct ledger ID" taggedAs securityAsset.setHappyCase(
    "Ledger API client can connect with the correct ledger ID"
  ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdminActualLedgerId))
  }
  it should "deny calls with a random ledger ID" taggedAs securityAsset.setAttack(
    attack(threat = "Exploit a random ledger ID")
  ) in {
    expectPermissionDenied(serviceCallWithToken(canReadAsAdminRandomLedgerId))
  }
  it should "allow calls with the correct participant ID" taggedAs securityAsset.setHappyCase(
    "Ledger API client can connect with the correct participant ID"
  ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdminActualParticipantId))
  }
  it should "deny calls with a random participant ID" taggedAs securityAsset.setAttack(
    attack(threat = "Exploit a random participant ID")
  ) in {
    expectPermissionDenied(serviceCallWithToken(canReadAsAdminRandomParticipantId))
  }
}
