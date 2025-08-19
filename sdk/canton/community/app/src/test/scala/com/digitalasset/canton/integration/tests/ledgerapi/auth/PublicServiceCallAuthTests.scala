// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.AuthServiceJWTPayload
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.{
  AuthInterceptorAndJWTSuppressionRule,
  AuthServiceJWTSuppressionRule,
}

import java.time.Duration
import java.util.UUID
import scala.concurrent.Future

trait PublicServiceCallAuthTests extends SecuredServiceCallAuthTests {

  protected override def prerequisiteParties: List[String] = List(randomParty)

  protected def serviceCallWithPayload(payload: AuthServiceJWTPayload)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    serviceCall(ServiceCallContext(Some(toHeader(payload))))

  serviceCallName should {
    "deny calls with an expired read-only token" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat = "Present an expired read-only JWT")
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(canReadAsRandomPartyExpired))
      }
    }

    "allow calls with explicitly non-expired read-only token" taggedAs securityAsset
      .setHappyCase("Ledger API client can make a call with token expiring tomorrow") in {
      implicit env =>
        import env.*
        expectSuccess(serviceCall(canReadAsRandomPartyExpiresTomorrow))
    }

    "allow calls with read-only token without expiration" taggedAs securityAsset
      .setHappyCase("Ledger API client can make a call with token without expiration") in {
      implicit env =>
        import env.*
        expectSuccess(serviceCall(canReadAsRandomParty))
    }

    "allow calls with 'participant_admin' user token" taggedAs securityAsset.setHappyCase(
      "Connect with `participant_admin` token"
    ) in { implicit env =>
      import env.*
      expectSuccess(serviceCall(canBeAnAdmin))
    }

    "allow calls with non-expired 'participant_admin' user token" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with non-expired `participant_admin` user token"
      ) in { implicit env =>
      import env.*
      val payload = standardToken(participantAdmin, Some(Duration.ofDays(1)))
      expectSuccess(serviceCallWithPayload(payload))
    }

    "deny calls with expired 'participant_admin' user token" taggedAs securityAsset
      .setAttack(
        attackUnauthenticated(threat = "Present an expired 'participant_admin' user JWT")
      ) in { implicit env =>
      import env.*
      val payload =
        standardToken(participantAdmin, Some(Duration.ofDays(-1)))
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCallWithPayload(payload))
      }
    }

    "allow calls with 'participant_admin' user token for this participant node" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with 'participant_admin' user token for this participant node"
      ) in { implicit env =>
      import env.*
      val payload =
        standardToken(
          userId = participantAdmin,
          participantId = Some(participant1.uid.toProtoPrimitive),
        )
      expectSuccess(serviceCallWithPayload(payload))
    }

    "deny calls with 'participant_admin' user token for another participant node" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present 'participant_admin' user JWT for another participant node"
        )
      ) in { implicit env =>
      import env.*
      val payload =
        standardToken(userId = participantAdmin, participantId = Some("other-participant-id"))
      expectPermissionDenied(serviceCallWithPayload(payload))
    }

    "allow calls with freshly created user" taggedAs securityAsset.setHappyCase(
      "allow calls with freshly created user"
    ) in { implicit env =>
      import env.*
      expectSuccess(
        createUserByAdmin(UUID.randomUUID().toString)
          .flatMap { case (_, context) => serviceCall(context) }
      )
    }
    "deny calls with non-expired 'unknown_user' user token" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present a non-expired 'unknown_user' user JWT")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthInterceptorAndJWTSuppressionRule) {
        expectPermissionDenied(serviceCall(canReadAsUnknownUserStandardJWT))
      }
    }

    "deny calls with an expired read/write token" taggedAs securityAsset.setAttack(
      attackPermissionDenied(threat = "Present an expired read/write JWT")
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(canActAsRandomPartyExpired))
      }
    }
    "allow calls with explicitly non-expired read/write token" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with explicitly non-expired read/write token"
      ) in { implicit env =>
      import env.*
      expectSuccess(serviceCall(canActAsRandomPartyExpiresTomorrow))
    }
    "allow calls with read/write token without expiration" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with read/write token without expiration"
      ) in { implicit env =>
      import env.*
      expectSuccess(serviceCall(canActAsRandomParty))
    }

    "deny calls with an expired admin token" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat = "Present an expired admin JWT")
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(canReadAsAdminExpired))
      }
    }
    "allow calls with explicitly non-expired admin token" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with explicitly non-expired admin token"
      ) in { implicit env =>
      import env.*
      expectSuccess(serviceCall(canReadAsAdminExpiresTomorrow))
    }

    "allow calls with admin token without expiration" taggedAs securityAsset.setHappyCase(
      "Ledger API client can make a call with admin token without expiration"
    ) in { implicit env =>
      import env.*
      expectSuccess(serviceCall(canBeAnAdmin))
    }
  }
}
