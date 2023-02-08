// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import scala.concurrent.Future
import com.daml.ledger.api.v1.admin.{user_management_service => ums}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

import java.util.UUID

trait GrantPermissionTest {

  this: AdminOrIDPAdminServiceCallAuthTests with UserManagementAuth =>

  val adminPermission =
    ums.Right(ums.Right.Kind.ParticipantAdmin(ums.Right.ParticipantAdmin()))

  val idpAdminPermission =
    ums.Right(ums.Right.Kind.IdentityProviderAdmin(ums.Right.IdentityProviderAdmin()))

  def serviceCallWithGrantPermission(
      context: ServiceCallContext,
      permission: ums.Right,
  ): Future[Any]

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    serviceCallWithGrantPermission(context, idpAdminPermission)

  private def grantPermissionCallWithIdpUser(
      rights: Vector[ums.Right.Kind],
      identityProviderId: String,
      tokenIssuer: Option[String],
      secret: Option[String] = None,
  ): Future[Any] =
    createUserByAdmin(
      userId = UUID.randomUUID().toString,
      identityProviderId = identityProviderId,
      tokenIssuer = tokenIssuer,
      rights = rights.map(ums.Right(_)),
      secret = secret,
    ).flatMap { case (_, context) =>
      serviceCallWithGrantPermission(context, adminPermission)
    }

  it should "deny calls to grant ParticipantAdmin by IdentityProviderAdmin user" taggedAs adminSecurityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Attempt to grant ParticipantAdmin permission by the user with only IdentityProviderAdmin right"
      )
    ) in {
    expectPermissionDenied {
      for {
        response <- createConfig(canReadAsAdminStandardJWT)
        identityProviderConfig = response.identityProviderConfig.getOrElse(
          sys.error("Failed to create idp config")
        )
        tokenIssuer = Some(identityProviderConfig.issuer)
        _ <- grantPermissionCallWithIdpUser(
          idpAdminRights,
          identityProviderConfig.identityProviderId,
          tokenIssuer,
        )
      } yield ()
    }
  }

}
