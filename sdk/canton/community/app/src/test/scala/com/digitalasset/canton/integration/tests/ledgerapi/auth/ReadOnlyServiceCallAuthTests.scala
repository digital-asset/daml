// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.{
  AuthInterceptorSuppressionRule,
  AuthServiceJWTSuppressionRule,
}
import org.scalatest.Assertion

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}

trait ReadOnlyServiceCallAuthTests extends ServiceCallWithMainActorAuthTests {

  def denyAdmin: Boolean = true;

  /** Allows to override what is regarded as a successful response, e.g. lookup queries for commands
    * can return a NOT_FOUND, which is fine because the result is not PERMISSION_DENIED
    */
  def successfulBehavior(f: Future[Any])(implicit ec: ExecutionContext): Assertion =
    expectSuccess(f)

  /** Flag to switch of a particular kind of test for technical reasons. See the use sites for
    * details.
    */
  protected val testCanReadAsMainActor: Boolean = true

  serviceCallName should {
    "deny calls with an expired token for a user that can-read-as main actor" taggedAs securityAsset
      .setAttack(
        attackUnauthenticated(threat =
          "Present an expired token for a user that can-read-as main actor"
        )
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(canReadAsMainActorExpired))
      }
    }

    "allow calls with explicitly non-expired token for a user that can-read-as main actor" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with explicitly non-expired token for a user that can-read-as main actor"
      ) in { implicit env =>
      import env.*
      assume(testCanReadAsMainActor)
      successfulBehavior(
        serviceCall(canReadAsMainActorExpiresInAnHour)
      )
    }

    "allow calls with token without expiration for a user that can-read-as main actor" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with token without expiration for a user that can-read-as main actor"
      ) in { implicit env =>
      import env.*
      // The completion stream tests are structured as submit-command-then-consume-completions, which requires read-write
      // rights. The tests for custom claim tokens provide the read-write tokens implicitly. That is not possible for user tokens.
      // We thus disable this test via an override in the completion stream tests.
      assume(testCanReadAsMainActor)
      successfulBehavior(
        serviceCallWithMainActorUser(
          "u3",
          Vector(proto.Right.Kind.CanReadAs(proto.Right.CanReadAs(getMainActorId))),
        )
      )
    }

    "deny calls with an expired token for a can-read-as-any-party user" taggedAs securityAsset
      .setAttack(
        attackUnauthenticated(threat = "Present an expired token for a can-read-as-any-party user")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(canReadAsAnyPartyExpired))
      }
    }

    "allow calls with explicitly non-expired token for a can-read-as-any-party user" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with explicitly non-expired token for a can-read-as-any-party user"
      ) in { implicit env =>
      import env.*
      assume(testCanReadAsMainActor)
      successfulBehavior(
        serviceCall(canReadAsAnyPartyExpiresInAnHour)
      )
    }

    "allow calls with token without expiration for a user that can read as any party" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with token without expiration for a can-read-as-any-party user"
      ) in { implicit env =>
      import env.*
      assume(testCanReadAsMainActor)
      successfulBehavior(
        serviceCall(
          canReadAsAnyParty
        )
      )
    }

    if (denyAdmin) {
      "deny calls with 'participant_admin' user token" taggedAs securityAsset.setAttack(
        attackPermissionDenied(threat = "Present a 'participant_admin' user token")
      ) in { implicit env =>
        import env.*
        expectPermissionDenied(
          serviceCall(canBeAnAdmin.copy())
        )
      }
    }

    "deny calls with user token that cannot read as main actor" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a user token with permission cannot read as main actor"
        )
      ) in { implicit env =>
      import env.*
      expectPermissionDenied(serviceCallWithMainActorUser("u4", Vector.empty))
    }

    "deny calls with non-expired 'unknown_user' user token" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present a non-expired 'unknown_user' user token")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthInterceptorSuppressionRule) {
        expectPermissionDenied(
          serviceCall(canReadAsUnknownUserStandardJWT.copy())
        )
      }
    }

    "deny calls with an expired token for a user that can-act-as main actor" taggedAs securityAsset
      .setAttack(
        attackUnauthenticated(threat =
          "Present an expired token for a user that can-act-as main actor"
        )
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(
          serviceCallWithMainActorUser(
            "u5",
            Vector(proto.Right.Kind.CanActAs(proto.Right.CanActAs(getMainActorId))),
            expiringIn(Duration.ofDays(-1), _),
          )
        )
      }
    }

    "allow calls with explicitly non-expired token for a user that can-act-as main actor" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with non-expired token for a user that can-act-as main actor"
      ) in { implicit env =>
      import env.*
      successfulBehavior(
        serviceCallWithMainActorUser(
          "u6",
          Vector(proto.Right.Kind.CanActAs(proto.Right.CanActAs(getMainActorId))),
          expiringIn(Duration.ofHours(1), _),
        )
      )
    }

    "allow calls with token without expiration for a user that can-act-as main actor" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with token without expiration for a user that can-act-as main actor"
      ) in { implicit env =>
      import env.*
      successfulBehavior(
        serviceCallWithMainActorUser(
          "u7",
          Vector(proto.Right.Kind.CanActAs(proto.Right.CanActAs(getMainActorId))),
        )
      )
    }
  }
}
