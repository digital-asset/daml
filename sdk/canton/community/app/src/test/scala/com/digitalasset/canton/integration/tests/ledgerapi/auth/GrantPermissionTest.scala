// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service as ums
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthInterceptorSuppressionRule

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

trait GrantPermissionTest {

  this: AdminOrIDPAdminServiceCallAuthTests with UserManagementAuth =>

  val adminPermission =
    ums.Right(ums.Right.Kind.ParticipantAdmin(ums.Right.ParticipantAdmin()))

  val idpAdminPermission =
    ums.Right(ums.Right.Kind.IdentityProviderAdmin(ums.Right.IdentityProviderAdmin()))

  def serviceCallWithGrantPermission(
      context: ServiceCallContext,
      permission: ums.Right,
  )(implicit ec: ExecutionContext): Future[Any]

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = {
    import env.*
    serviceCallWithGrantPermission(context, idpAdminPermission)
  }

  private def grantPermissionCallWithIdpUser(
      rights: Vector[ums.Right.Kind],
      identityProviderId: String,
      tokenIssuer: Option[String],
      secret: Option[String] = None,
  )(implicit ec: ExecutionContext): Future[Any] =
    createUserByAdmin(
      userId = UUID.randomUUID().toString,
      identityProviderId = identityProviderId,
      tokenIssuer = tokenIssuer,
      rights = rights.map(ums.Right(_)),
      secret = secret,
    ).flatMap { case (_, context) =>
      serviceCallWithGrantPermission(context, adminPermission)
    }

  serviceCallName should {
    "deny calls to grant ParticipantAdmin by IdentityProviderAdmin user" taggedAs adminSecurityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Attempt to grant ParticipantAdmin permission by the user with only IdentityProviderAdmin right"
        )
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthInterceptorSuppressionRule) {
        expectPermissionDenied {
          for {
            idpConfig <- createConfig(canBeAnAdmin)
            tokenIssuer = Some(idpConfig.issuer)
            _ <- grantPermissionCallWithIdpUser(
              idpAdminRights,
              idpConfig.identityProviderId,
              tokenIssuer,
            )
          } yield ()
        }
      }
    }
  }

}
