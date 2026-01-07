// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthServiceJWTSuppressionRule

trait ExecuteAsAuthTests { self: ServiceCallWithMainActorAuthTests =>

  def executeAsShouldSucceed: Boolean = false

  serviceCallName should {

    if (executeAsShouldSucceed) {
      "allow calls with execute token without expiration" taggedAs securityAsset
        .setHappyCase(
          "Ledger API client can make a call with execute token without expiration"
        ) in { implicit env =>
        import env.*
        expectInvalidArgument(serviceCall(canExecuteAsMainActor))
      }
      "allow calls with execute as any party token without expiration" taggedAs securityAsset
        .setHappyCase(
          "Ledger API client can make a call with execute token without expiration"
        ) in { implicit env =>
        import env.*
        expectInvalidArgument(serviceCall(canExecuteAsAnyParty))
      }
    } else {
      "deny calls with execute token without expiration" taggedAs securityAsset.setAttack(
        attackPermissionDenied(threat = "Present a token entitling to execute but not to read")
      ) in { implicit env =>
        import env.*
        loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
          expectPermissionDenied(serviceCall(canExecuteAsMainActor))
        }
      }
      "deny calls with execute as any party token without expiration" taggedAs securityAsset
        .setAttack(
          attackPermissionDenied(threat =
            "Present a token entitling to execute as any party but not to read"
          )
        ) in { implicit env =>
        import env.*
        loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
          expectPermissionDenied(serviceCall(canExecuteAsAnyParty))
        }
      }
    }
  }

}
