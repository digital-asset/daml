// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.identity_provider_config_service.IdentityProviderConfig
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.user_management_service.User
import com.daml.ledger.api.v2.admin.{
  party_management_service as pproto,
  user_management_service as uproto,
}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.{
  ApiPartyManagementServiceSuppressionRule,
  AuthServiceJWTSuppressionRule,
}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

final class AllocatePartyBoxToIDPAuthIT
    extends ServiceCallAuthTests
    with IdentityProviderConfigAuth
    with ErrorsAssertions {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String =
    "PartyManagementService#AllocateParty(<grant-rights-to-IDP-parties>)"

  override protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

  protected def createUser(
      userId: String,
      serviceCallContext: ServiceCallContext,
  ): Future[uproto.CreateUserResponse] = {
    val user = uproto.User(
      id = userId,
      primaryParty = "",
      isDeactivated = false,
      metadata = Some(ObjectMeta.defaultInstance),
      identityProviderId = serviceCallContext.identityProviderId,
    )
    val req = uproto.CreateUserRequest(Some(user), Vector.empty)
    stub(uproto.UserManagementServiceGrpc.stub(channel), serviceCallContext.token)
      .createUser(req)
  }

  private def allocateParty(
      party: String,
      userId: String,
      serviceCallContext: ServiceCallContext,
      identityProviderIdOverride: Option[String] = None,
  )(implicit
      ec: ExecutionContext
  ): Future[String] =
    stub(pproto.PartyManagementServiceGrpc.stub(channel), serviceCallContext.token)
      .allocateParty(
        pproto.AllocatePartyRequest(
          partyIdHint = party,
          localMetadata = None,
          identityProviderId =
            identityProviderIdOverride.getOrElse(serviceCallContext.identityProviderId),
          synchronizerId = "",
          userId = userId,
        )
      )
      .map(_.partyDetails.value.party)

  private def createIDPBundle(context: ServiceCallContext, suffix: String)(implicit
      ec: ExecutionContext
  ): Future[(User, ServiceCallContext, IdentityProviderConfig)] =
    for {
      idpConfig <- createConfig(context)
      (user, idpAdminContext) <- createUserByAdminRSA(
        userId = "idp-admin-" + suffix,
        identityProviderId = idpConfig.identityProviderId,
        tokenIssuer = Some(idpConfig.issuer),
        rights = idpAdminRights,
        privateKey = key1.privateKey,
        keyId = key1.id,
      )
    } yield (user, idpAdminContext, idpConfig)

  serviceCallName should {
    "deny IDP Admin granting permissions to users which do not exist" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant rights to non existing users")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnknownResource {
          val suffix = UUID.randomUUID().toString
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            _ <- allocateParty(
              "party-" + suffix,
              "user-" + suffix,
              idpAdminContext,
            )
          } yield ()
        }
      }
    }

    "allow IDP Admin granting permissions to users which are allocated in the same IDP" taggedAs adminSecurityAsset
      .setHappyCase(
        "IDP admin can grant user rights to parties in the same IDP"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          val userId = "user-" + suffix
          for {
            (_, idpAdminContext, idpConfig) <- createIDPBundle(canBeAnAdmin, suffix)
            _ <- createUser(userId, idpAdminContext)

            _ <- allocateParty(
              "party-" + suffix,
              userId,
              idpAdminContext,
            )
          } yield ()
        }
      }
    }

    "deny IDP Admin granting permissions to users which are outside of any IDP" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant user rights to users outside of any IDP")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(
        AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
      ) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          val userId = "user-" + suffix
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            _ <- createUser(userId, canBeAnAdmin)

            _ <- allocateParty(
              "party-" + suffix,
              userId,
              idpAdminContext,
            )
          } yield ()
        }
      }
    }

    "deny IDP Admin granting permissions in other IDPs" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant rights to parties in other IDPs")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(
        AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
      ) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          val userId = "user-" + suffix
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            (_, otherIdpAdminContext, _) <- createIDPBundle(canBeAnAdmin, "other-" + suffix)
            _ <- createUser(userId, otherIdpAdminContext)

            _ <- allocateParty(
              "party-" + suffix,
              userId,
              idpAdminContext,
            )
          } yield ()
        }
      }
    }

    "deny Admin granting permissions to users which do not exist" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant rights to non existing users")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnknownResource {
          val suffix = UUID.randomUUID().toString
          for {
            _ <- allocateParty(
              "party-" + suffix,
              "user-" + suffix,
              canBeAnAdmin,
            )
          } yield ()
        }
      }
    }

    "allow Admin granting permissions to users which are allocated in the same IDP" taggedAs adminSecurityAsset
      .setHappyCase(
        "Grant user rights to parties in the same IDP"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val suffix = UUID.randomUUID().toString
          val userId = "user-" + suffix
          for {
            (_, idpAdminContext, idpConfig) <- createIDPBundle(canBeAnAdmin, suffix)
            _ <- createUser(userId, idpAdminContext)

            _ <- allocateParty(
              "party-" + suffix,
              userId,
              canBeAnAdmin,
              Some(idpAdminContext.identityProviderId),
            )
          } yield ()
        }
      }
    }

    "deny Admin granting permissions to users which are outside of any IDP" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant user rights to users outside of any IDP")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(
        AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
      ) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          val userId = "user-" + suffix
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            _ <- createUser(userId, canBeAnAdmin)

            _ <- allocateParty(
              "party-" + suffix,
              userId,
              canBeAnAdmin,
              Some(idpAdminContext.identityProviderId),
            )
          } yield ()
        }
      }
    }

    "deny Admin granting permissions in other IDPs" taggedAs adminSecurityAsset
      .setAttack(
        attackUnknownResource(threat = "Grant rights to parties in other IDPs")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(
        AuthServiceJWTSuppressionRule || ApiPartyManagementServiceSuppressionRule
      ) {
        expectPermissionDenied {
          val suffix = UUID.randomUUID().toString
          val userId = "user-" + suffix
          for {
            (_, idpAdminContext, _) <- createIDPBundle(canBeAnAdmin, suffix)
            (_, otherIdpAdminContext, _) <- createIDPBundle(canBeAnAdmin, "other-" + suffix)
            _ <- createUser(userId, otherIdpAdminContext)

            _ <- allocateParty(
              "party-" + suffix,
              userId,
              canBeAnAdmin,
              Some(idpAdminContext.identityProviderId),
            )
          } yield ()
        }
      }
    }
  }

  protected def idpAdminRights: Vector[uproto.Right] = Vector(
    uproto.Right(
      uproto.Right.Kind.IdentityProviderAdmin(uproto.Right.IdentityProviderAdmin())
    )
  )

}
