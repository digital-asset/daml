// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service.*
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.ApiUserManagementServiceSuppressionRule
import io.grpc.Status

import java.util.UUID
import scala.concurrent.Future

class ListUserRightsAuthIT extends AdminOrIDPAdminServiceCallAuthTests with UserManagementAuth {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "UserManagementService#ListUserRights"

  def getRights(
      context: ServiceCallContext,
      userId: String,
      idpId: Option[String] = None,
  ): Future[ListUserRightsResponse] =
    stub(context.token).listUserRights(
      ListUserRightsRequest(
        userId,
        identityProviderId = idpId.getOrElse(context.identityProviderId),
      )
    )

  // admin and idp admin users are allowed to specify a user-id other than their own for which to retrieve a user
  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    import context.*
    val userId = "alice-" + UUID.randomUUID().toString
    for {
      // create a normal user
      (alice, _) <- createUserByAdmin(userId, identityProviderId)
      _ <- getRights(context, alice.id)
    } yield ()
  }

  serviceCallName should {
    "deny querying for non-existent user" taggedAs adminSecurityAsset
      .setAttack(
        attackPermissionDenied(threat = "query rights of a non-existent user")
      ) in { implicit env =>
      import env.*
      expectFailure(
        {
          val userId = "fresh-user-" + UUID.randomUUID().toString
          for {
            _ <- createUserByAdmin(userId)
            _ <- getRights(canBeAnAdmin, "non-existent-user-" + UUID.randomUUID().toString)
          } yield ()
        },
        Status.Code.NOT_FOUND,
      )
    }
    "allow user to query own rights" taggedAs adminSecurityAsset
      .setHappyCase(
        "User can query own rights"
      ) in { implicit env =>
      import env.*
      expectSuccess {
        val userId = "fresh-user-" + UUID.randomUUID().toString
        for {
          (user, userContext) <- createUserByAdmin(userId)
          _ <- getRights(userContext, user.id)
        } yield ()
      }
    }
    "deny user to query own rights when idp doesn't match" taggedAs adminSecurityAsset
      .setHappyCase(
        "User can query own rights"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(ApiUserManagementServiceSuppressionRule) {
        expectPermissionDenied {
          val userId = "fresh-user-" + UUID.randomUUID().toString
          for {
            (user, userContext) <- createUserByAdmin(userId)
            _ <- getRights(userContext, user.id, idpId = Some(UUID.randomUUID().toString))
          } yield ()
        }
      }
    }
  }
}
