// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.time.Duration
import java.util.UUID
import com.daml.ledger.api.auth.AuthServiceJWTPayload
import com.daml.security.evidence.tag.Security.{Attack, SecurityTest}
import com.daml.security.evidence.tag.Security.SecurityTest.Property.SecureConfiguration
import com.daml.ledger.security.test.SystematicTesting._
import scala.concurrent.Future

trait PublicServiceCallAuthTests extends SecuredServiceCallAuthTests {

  protected def serviceCallWithPayload(payload: AuthServiceJWTPayload): Future[Any] =
    serviceCallWithToken(Some(toHeader(payload)))

  val securityAsset: SecurityTest =
    SecurityTest(property = SecureConfiguration, asset = serviceCallName)

  def attack(threat: String): Attack = Attack(
    actor = "A user that can reach the ledger api",
    threat = threat,
    mitigation = "Refuse to connect the user to the participant node",
  )

  it should "deny calls with an expired read-only token" taggedAs securityAsset.setAttack(
    attack(threat = "Exploit an expired token")
  ) in {
    expectUnauthenticated(serviceCallWithToken(canReadAsRandomPartyExpired))
  }

  it should "allow calls with explicitly non-expired read-only token" taggedAs securityAsset
    .setHappyCase("Connect with token expiring tomorrow") in {
    expectSuccess(serviceCallWithToken(canReadAsRandomPartyExpiresTomorrow))
  }
  it should "allow calls with read-only token without expiration" taggedAs securityAsset
    .setHappyCase("Connect with token without expiration") in {
    expectSuccess(serviceCallWithToken(canReadAsRandomParty))
  }

  it should "allow calls with 'participant_admin' user token" taggedAs securityAsset.setHappyCase(
    "Connect with `participant_admin` token"
  ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdminStandardJWT))
  }
  it should "allow calls with non-expired 'participant_admin' user token" in {
    val payload = standardToken("participant_admin", Some(Duration.ofDays(1)))
    expectSuccess(serviceCallWithPayload(payload))
  }
  it should "deny calls with expired 'participant_admin' user token" in {
    val payload =
      standardToken("participant_admin", Some(Duration.ofDays(-1)))
    expectUnauthenticated(serviceCallWithPayload(payload))
  }
  it should "allow calls with 'participant_admin' user token for this participant node" in {
    val payload =
      standardToken(userId = "participant_admin", participantId = Some("sandbox-participant"))
    expectSuccess(serviceCallWithPayload(payload))
  }
  it should "deny calls with 'participant_admin' user token for another participant node" in {
    val payload =
      standardToken(userId = "participant_admin", participantId = Some("other-participant-id"))
    expectPermissionDenied(serviceCallWithPayload(payload))
  }
  it should "allow calls with freshly created user" in {
    expectSuccess(
      createUserByAdmin(UUID.randomUUID().toString)
        .flatMap { case (_, token) => serviceCallWithToken(token) }
    )
  }
  it should "deny calls with non-expired 'unknown_user' user token" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsUnknownUserStandardJWT))
  }

  it should "deny calls with an expired read/write token" in {
    expectUnauthenticated(serviceCallWithToken(canActAsRandomPartyExpired))
  }
  it should "allow calls with explicitly non-expired read/write token" in {
    expectSuccess(serviceCallWithToken(canActAsRandomPartyExpiresTomorrow))
  }
  it should "allow calls with read/write token without expiration" in {
    expectSuccess(serviceCallWithToken(canActAsRandomParty))
  }

  it should "deny calls with an expired admin token" in {
    expectUnauthenticated(serviceCallWithToken(canReadAsAdminExpired))
  }
  it should "allow calls with explicitly non-expired admin token" in {
    expectSuccess(serviceCallWithToken(canReadAsAdminExpiresTomorrow))
  }
  it should "allow calls with admin token without expiration" in {
    expectSuccess(serviceCallWithToken(canReadAsAdmin))
  }

  it should "allow calls with the correct ledger ID" in {
    expectSuccess(serviceCallWithToken(canReadAsRandomPartyActualLedgerId))
  }
  it should "deny calls with a random ledger ID" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsRandomPartyRandomLedgerId))
  }
  it should "allow calls with the correct participant ID" in {
    expectSuccess(serviceCallWithToken(canReadAsRandomPartyActualParticipantId))
  }
  it should "deny calls with a random participant ID" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsRandomPartyRandomParticipantId))
  }
}
