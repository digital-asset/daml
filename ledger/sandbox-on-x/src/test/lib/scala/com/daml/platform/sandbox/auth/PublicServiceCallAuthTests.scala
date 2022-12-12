// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.time.Duration
import java.util.UUID
import com.daml.ledger.api.auth.AuthServiceJWTPayload
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import scala.concurrent.Future

trait PublicServiceCallAuthTests extends SecuredServiceCallAuthTests {

  protected override def prerequisiteParties: List[String] = List(randomParty)

  protected def serviceCallWithPayload(payload: AuthServiceJWTPayload): Future[Any] =
    serviceCallWithToken(Some(toHeader(payload)))

  it should "deny calls with an expired read-only token" taggedAs securityAsset.setAttack(
    attackUnauthenticated(threat = "Present an expired read-only JWT")
  ) in {
    expectUnauthenticated(serviceCallWithToken(canReadAsRandomPartyExpired))
  }

  it should "allow calls with explicitly non-expired read-only token" taggedAs securityAsset
    .setHappyCase("Ledger API client can make a call with token expiring tomorrow") in {
    expectSuccess(serviceCallWithToken(canReadAsRandomPartyExpiresTomorrow))
  }

  it should "allow calls with read-only token without expiration" taggedAs securityAsset
    .setHappyCase("Ledger API client can make a call with token without expiration") in {
    expectSuccess(serviceCallWithToken(canReadAsRandomParty))
  }

  it should "allow calls with 'participant_admin' user token" taggedAs securityAsset.setHappyCase(
    "Connect with `participant_admin` token"
  ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdminStandardJWT))
  }

  it should "allow calls with non-expired 'participant_admin' user token" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with non-expired `participant_admin` user token"
    ) in {
    val payload = standardToken("participant_admin", Some(Duration.ofDays(1)))
    expectSuccess(serviceCallWithPayload(payload))
  }

  it should "deny calls with expired 'participant_admin' user token" taggedAs securityAsset
    .setAttack(
      attackUnauthenticated(threat = "Present an expired 'participant_admin' user JWT")
    ) in {
    val payload =
      standardToken("participant_admin", Some(Duration.ofDays(-1)))
    expectUnauthenticated(serviceCallWithPayload(payload))
  }

  it should "allow calls with 'participant_admin' user token for this participant node" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with 'participant_admin' user token for this participant node"
    ) in {
    val payload =
      standardToken(userId = "participant_admin", participantId = Some("sandbox-participant"))
    expectSuccess(serviceCallWithPayload(payload))
  }

  it should "deny calls with 'participant_admin' user token for another participant node" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Present 'participant_admin' user JWT for another participant node"
      )
    ) in {
    val payload =
      standardToken(userId = "participant_admin", participantId = Some("other-participant-id"))
    expectPermissionDenied(serviceCallWithPayload(payload))
  }

  it should "allow calls with freshly created user" taggedAs securityAsset.setHappyCase(
    "allow calls with freshly created user"
  ) in {
    expectSuccess(
      createUserByAdmin(UUID.randomUUID().toString)
        .flatMap { case (_, token) => serviceCallWithToken(token) }
    )
  }
  it should "deny calls with non-expired 'unknown_user' user token" taggedAs securityAsset
    .setAttack(
      attackPermissionDenied(threat = "Present a non-expired 'unknown_user' user JWT")
    ) in {
    expectPermissionDenied(serviceCallWithToken(canReadAsUnknownUserStandardJWT))
  }

  it should "deny calls with an expired read/write token" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Present an expired read/write JWT")
  ) in {
    expectUnauthenticated(serviceCallWithToken(canActAsRandomPartyExpired))
  }
  it should "allow calls with explicitly non-expired read/write token" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with explicitly non-expired read/write token"
    ) in {
    expectSuccess(serviceCallWithToken(canActAsRandomPartyExpiresTomorrow))
  }
  it should "allow calls with read/write token without expiration" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with read/write token without expiration"
    ) in {
    expectSuccess(serviceCallWithToken(canActAsRandomParty))
  }

  it should "deny calls with an expired admin token" taggedAs securityAsset.setAttack(
    attackUnauthenticated(threat = "Present an expired admin JWT")
  ) in {
    expectUnauthenticated(serviceCallWithToken(canReadAsAdminExpired))
  }
  it should "allow calls with explicitly non-expired admin token" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with explicitly non-expired admin token"
    ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdminExpiresTomorrow))
  }

  it should "allow calls with admin token without expiration" taggedAs securityAsset.setHappyCase(
    "Ledger API client can make a call with admin token without expiration"
  ) in {
    expectSuccess(serviceCallWithToken(canReadAsAdmin))
  }

  it should "allow calls with the correct ledger ID" taggedAs securityAsset.setHappyCase(
    "Ledger API client can make a call with the correct ledger ID"
  ) in {
    expectSuccess(serviceCallWithToken(canReadAsRandomPartyActualLedgerId))
  }

  it should "deny calls with a random ledger ID" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Present a JWT with an unknown ledger ID")
  ) in {
    expectPermissionDenied(serviceCallWithToken(canReadAsRandomPartyRandomLedgerId))
  }

  it should "allow calls with the correct participant ID" taggedAs securityAsset.setHappyCase(
    "Ledger API client can make a call with the correct participant ID"
  ) in {
    expectSuccess(serviceCallWithToken(canReadAsRandomPartyActualParticipantId))
  }

  it should "deny calls with a random participant ID" taggedAs securityAsset.setAttack(
    attackPermissionDenied(threat = "Present a JWT with an unknown participant ID")
  ) in {
    expectPermissionDenied(serviceCallWithToken(canReadAsRandomPartyRandomParticipantId))
  }
}
