// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service.*
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.ApiUserManagementServiceSuppressionRule
import io.grpc.Status

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class GetUserAuthIT extends AdminOrIDPAdminServiceCallAuthTests with UserManagementAuth {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "UserManagementService#GetUser"

  def getUser(context: ServiceCallContext, userId: String, idpId: Option[String] = None)(implicit
      ec: ExecutionContext
  ): Future[User] =
    stub(UserManagementServiceGrpc.stub(channel), context.token)
      .getUser(
        GetUserRequest(userId, identityProviderId = idpId.getOrElse(context.identityProviderId))
      )
      .map(_.user.value)

  // admin and idp admin users are allowed to specify a user-id other than their own for which to retrieve a user
  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    val userId = "alice-" + UUID.randomUUID().toString
    for {
      (alice, _) <- createUserByAdmin(userId, identityProviderId = context.identityProviderId)
      _ <- getUser(context, alice.id)
    } yield ()
  }

  serviceCallName should {
    "deny querying for non-existent user" taggedAs adminSecurityAsset
      .setAttack(
        attackPermissionDenied(threat = "query record of a non-existent user")
      ) in { implicit env =>
      import env.*
      expectFailure(
        {
          val userId = "fresh-user-" + UUID.randomUUID().toString
          for {
            _ <- createUserByAdmin(userId)
            _ <- getUser(canBeAnAdmin, "non-existent-user-" + UUID.randomUUID().toString)
          } yield ()
        },
        Status.Code.NOT_FOUND,
      )
    }
    "allow user to query own record" taggedAs adminSecurityAsset
      .setHappyCase(
        "User can query own record"
      ) in { implicit env =>
      import env.*
      expectSuccess {
        val userId = "fresh-user-" + UUID.randomUUID().toString
        for {
          (user, userContext) <- createUserByAdmin(userId)
          _ <- getUser(userContext, user.id)
        } yield ()
      }
    }
    "deny user to query own record when idp doesn't match" taggedAs adminSecurityAsset
      .setHappyCase(
        "User can query own record"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(ApiUserManagementServiceSuppressionRule) {
        expectPermissionDenied {
          val userId = "fresh-user-" + UUID.randomUUID().toString
          for {
            (user, userContext) <- createUserByAdmin(userId)
            _ <- getUser(userContext, user.id, idpId = Some(UUID.randomUUID().toString))
          } yield ()
        }
      }
    }
    "deny calls if user is created already within another IDP" taggedAs adminSecurityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present an existing userId but foreign Identity Provider")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(ApiUserManagementServiceSuppressionRule) {
        expectPermissionDenied {
          val userId = "fresh-user-" + UUID.randomUUID().toString
          for {
            idpConfig1 <- createConfig(canBeAnAdmin)
            idpConfig2 <- createConfig(canBeAnAdmin)
            _ <- createFreshUser(
              userId,
              canBeAnAdmin.token,
              idpConfig1.identityProviderId,
              Seq.empty,
            )
            _ <- stub(UserManagementServiceGrpc.stub(channel), canBeAnAdmin.token)
              .getUser(
                GetUserRequest(
                  userId,
                  identityProviderId = idpConfig2.identityProviderId,
                )
              )
              .map(_.user.value)
          } yield ()
        }
      }
    }
  }
}
