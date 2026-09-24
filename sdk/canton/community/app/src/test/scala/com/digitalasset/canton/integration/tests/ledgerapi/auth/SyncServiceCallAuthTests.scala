// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.{
  AuthInterceptorSuppressionRule,
  AuthServiceJWTSuppressionRule,
}
import org.scalatest.Assertion

import scala.concurrent.{ExecutionContext, Future}

trait SyncServiceCallAuthTests extends ServiceCallWithMainActorAuthTests {

  /** Allows to override what is regarded as a successful response, e.g. lookup queries for commands
    * can return a NOT_FOUND, which is fine because the result is not PERMISSION_DENIED
    */
  def successfulBehavior(f: Future[Any])(implicit ec: ExecutionContext): Assertion =
    expectSuccess(f)

  protected def serviceCallWithMainActorUser(
      userPrefix: String,
      right: proto.Right.Kind,
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    createUserByAdmin(userPrefix + mainActor, rights = Vector(proto.Right(right)))
      .flatMap { case (_, context) =>
        serviceCall(context.copy())
      }
  }

  serviceCallName should {
    "deny calls with an expired read/write token" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat = "Present an expired read/write JWT")
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(canActAsMainActorExpired))
      }
    }
    "allow calls with explicitly non-expired read/write token" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with explicitly non-expired read/write token"
      ) in { implicit env =>
      import env.*
      successfulBehavior(serviceCall(canActAsMainActorExpiresInAnHour))
    }
    "allow calls with read/write token without expiration" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with read/write token without expiration"
      ) in { implicit env =>
      import env.*
      successfulBehavior(serviceCall(canActAsMainActor))
    }
    "deny calls with user token that can-read-as main actor" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present a user JWT that can-read-as main actor")
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(
        serviceCallWithMainActorUser(
          "u2",
          proto.Right.Kind.CanReadAs(proto.Right.CanReadAs(getMainActorId)),
        )
      )
    }
    "deny calls with user token that can-read-as-any-party" taggedAs securityAsset.setAttack(
      attackPermissionDenied(threat = "Present a user token that can-read-as-any-party")
    ) in { implicit env =>
      import env.*
      expectPermissionDenied(serviceCall(canReadAsAnyParty))
    }
    "deny calls with 'participant_admin' user token" taggedAs securityAsset.setAttack(
      attackPermissionDenied(threat = "Present a 'participant_admin' user JWT")
    ) in { implicit env =>
      import env.*
      expectPermissionDenied(serviceCall(canBeAnAdmin))
    }
    "deny calls with non-expired 'unknown_user' user token" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present a non-expired 'unknown_user' user JWT")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthInterceptorSuppressionRule) {
        expectPermissionDenied(serviceCall(canReadAsUnknownUserStandardJWT))
      }
    }
    "deny calls with explicitly non-expired read-only token" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present a explicitly non-expired read-only JWT")
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(serviceCall(canReadAsMainActorExpiresInAnHour))
    }
    "deny calls with read-only token without expiration" taggedAs securityAsset.setAttack(
      attackPermissionDenied(threat = "Present a read-only JWT without expiration")
    ) in { implicit env =>
      import env.*
      expectPermissionDenied(serviceCall(canReadAsMainActor))
    }

    "deny calls with a random user ID" taggedAs securityAsset.setAttack(
      attackPermissionDenied(threat = "Present a JWT with an unknown user ID")
    ) in { implicit env =>
      import env.*
      expectPermissionDenied(serviceCall(canActAsRandomParty))
    }
    "allow calls with an user ID present in the message and a token with an empty user ID" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with an user ID present in the message and a token with an empty user ID"
      ) in { implicit env =>
      import env.*
      successfulBehavior(
        serviceCall(
          canActAsMainActor.copy()
        )
      )
    }
  }
}
