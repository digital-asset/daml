// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.StandardJWTTokenFormat
import com.daml.ledger.api.v2.admin.identity_provider_config_service.{
  IdentityProviderConfig,
  UpdateIdentityProviderConfigRequest,
}
import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.{
  AuthInterceptorSuppressionRule,
  AuthServiceJWTSuppressionRule,
  IDPAndJWTSuppressionRule,
}
import com.google.protobuf.field_mask.FieldMask

import java.security.interfaces.RSAPrivateKey
import java.util.UUID

trait AdminOrIDPAdminServiceCallAuthTests
    extends AdminServiceCallAuthTests
    with IdentityProviderConfigAuth {

  override def denyCallsWithIdpAdmin: Boolean = false

  serviceCallName should {
    "allow IDP Admin to operate within own IDP" taggedAs adminSecurityAsset
      .setHappyCase(
        "Ledger API client can make a call with IDP admin user"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          for {
            idpConfig <- createConfig(canBeAnAdmin)
            _ <- serviceCallWithIDPUser(
              userId = UUID.randomUUID().toString,
              rights = idpAdminRights,
              identityProviderId = idpConfig.identityProviderId,
              tokenIssuer = Some(idpConfig.issuer),
              privateKey = Some(key1.privateKey),
              keyId = key1.id,
            )
          } yield ()
        }
      }
    }

    "allow IDP Admin to operate within own IDP providing intended audience" taggedAs adminSecurityAsset
      .setHappyCase(
        "Ledger API client can make a call with freshly created IDP admin user"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectSuccess {
          val userId = UUID.randomUUID().toString
          for {
            idpConfig <- createConfig(canBeAnAdmin, audience = Some(UUID.randomUUID().toString))
            (_, _) <- createUserByAdmin(
              userId = userId,
              rights = idpAdminRights.map(proto.Right(_)),
              identityProviderId = idpConfig.identityProviderId,
            )
            token = audienceBasedToken(
              userId,
              key2.privateKey,
              key2.id,
              idpConfig,
              audience = List(idpConfig.audience),
            )
            context = ServiceCallContext(token, None, idpConfig.identityProviderId)
            _ <- serviceCall(context)
          } yield ()
        }
      }
    }

    "deny IDP Admin to operate within own IDP while missing intended audience" taggedAs adminSecurityAsset
      .setAttack(attackPermissionDenied(threat = "Present a JWT with a missing audience")) in {
      implicit env =>
        import env.*
        loggerFactory.suppress(IDPAndJWTSuppressionRule) {
          expectUnauthenticated {
            val userId = UUID.randomUUID().toString
            for {
              idpConfig <- createConfig(canBeAnAdmin, audience = Some(UUID.randomUUID().toString))
              (_, _) <- createUserByAdmin(
                userId = userId,
                rights = idpAdminRights.map(proto.Right(_)),
                identityProviderId = idpConfig.identityProviderId,
              )
              token = audienceBasedToken(
                userId,
                key2.privateKey,
                key2.id,
                idpConfig,
                audience = List.empty,
              )
              context = ServiceCallContext(token, None, idpConfig.identityProviderId)
              _ <- serviceCall(context)
            } yield ()
          }
        }
    }

    "deny IDP Admin to operate within own IDP while missing issuer" taggedAs adminSecurityAsset
      .setAttack(attackPermissionDenied(threat = "Present a JWT with a missing issuer")) in {
      implicit env =>
        import env.*
        loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
          expectUnauthenticated {
            for {
              idpConfig <- createConfig(canBeAnAdmin)
              _ <- serviceCallWithIDPUser(
                userId = UUID.randomUUID().toString,
                rights = idpAdminRights,
                identityProviderId = idpConfig.identityProviderId,
                tokenIssuer = None,
              )
            } yield ()
          }
        }
    }

    "deny IDP Admin to operate within own IDP that has been deactivated" taggedAs adminSecurityAsset
      .setAttack(attackPermissionDenied(threat = "Present a JWT of inactive IDP")) in {
      implicit env =>
        import env.*
        loggerFactory.suppress(AuthInterceptorSuppressionRule) {
          expectPermissionDenied {
            for {
              idpConfig <- createConfig(canBeAnAdmin)
              (_, context) <- createUserByAdmin(
                userId = UUID.randomUUID().toString,
                identityProviderId = idpConfig.identityProviderId,
                tokenIssuer = Some(idpConfig.issuer),
                rights = idpAdminRights.map(proto.Right(_)),
              )
              _ <- idpStub(context).updateIdentityProviderConfig(
                UpdateIdentityProviderConfigRequest(
                  identityProviderConfig = Some(idpConfig.copy(isDeactivated = true)),
                  updateMask = Some(FieldMask(scala.Seq("is_deactivated"))),
                )
              )
              _ <- serviceCall(context)
            } yield ()
          }
        }
    }

    "deny IDP User to operate within own IDP while missing IDP Admin right" taggedAs adminSecurityAsset
      .setAttack(attackPermissionDenied(threat = "Act without IDP Admin permission")) in {
      implicit env =>
        import env.*
        loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
          expectPermissionDenied {
            for {
              idpConfig <- createConfig(canBeAnAdmin)
              _ <- serviceCallWithIDPUser(
                userId = UUID.randomUUID().toString,
                rights = Vector(),
                identityProviderId = idpConfig.identityProviderId,
                tokenIssuer = Some(idpConfig.issuer),
                privateKey = Some(key2.privateKey),
                keyId = key2.id,
              )
            } yield ()
          }
        }
    }

    "deny IDP Admin to operate within another IDP" taggedAs adminSecurityAsset
      .setAttack(attackPermissionDenied(threat = "Act within non-authorized IDP")) in {
      implicit env =>
        import env.*
        loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
          expectPermissionDenied {
            for {
              idpConfig1 <- createConfig(canBeAnAdmin)
              idpConfig2 <- createConfig(canBeAnAdmin)
              _ <- createUserByAdminRSA(
                userId = UUID.randomUUID().toString,
                identityProviderId = idpConfig1.identityProviderId,
                tokenIssuer = Some(idpConfig1.issuer),
                rights = idpAdminRights.map(proto.Right(_)),
                primaryParty = "some-party-1",
                privateKey = key1.privateKey,
                keyId = key1.id,
              ).flatMap { case (_, context) =>
                serviceCall(context.copy(identityProviderId = idpConfig2.identityProviderId))
              }
            } yield ()
          }
        }
    }

    "deny calls with user token signed by private key of another IDP" taggedAs adminSecurityAsset
      .setAttack(attackPermissionDenied(threat = "Present a JWT signed by wrong private key")) in {
      implicit env =>
        import env.*
        loggerFactory.suppress(IDPAndJWTSuppressionRule) {
          expectUnauthenticated {
            for {
              idpConfig <- createConfig(canBeAnAdmin)
              _ <- serviceCallWithIDPUser(
                userId = UUID.randomUUID().toString,
                rights = idpAdminRights,
                identityProviderId = idpConfig.identityProviderId,
                tokenIssuer = Some(idpConfig.issuer),
                privateKey = Some(key3.privateKey),
                keyId = key3.id,
              )
            } yield ()
          }
        }
    }

    "deny calls if Jwks url is unreachable" taggedAs adminSecurityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Present a JWT which cannot be validated as Jwks URL is unreachable"
        )
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(IDPAndJWTSuppressionRule) {
        expectUnauthenticated {
          for {
            idpConfig <- createConfig(
              canBeAnAdmin,
              jwksUrlOverride = Some("http://localhost:12345/unreachableURL"),
            )
            _ <- serviceCallWithIDPUser(
              userId = UUID.randomUUID().toString,
              rights = idpAdminRights,
              identityProviderId = idpConfig.identityProviderId,
              tokenIssuer = Some(idpConfig.issuer),
            )
          } yield ()
        }
      }
    }
  }

  private def audienceBasedToken(
      userId: String,
      privateKey: RSAPrivateKey,
      keyId: String,
      identityProviderConfig: IdentityProviderConfig,
      audience: List[String],
  ) =
    Some(
      toHeaderRSA(
        keyId,
        standardToken(userId, issuer = Some(identityProviderConfig.issuer))
          .copy(
            audiences = audience,
            format = StandardJWTTokenFormat.Audience,
          ),
        privateKey = privateKey,
        enforceFormat = Some(StandardJWTTokenFormat.Audience),
      )
    )

  protected def idpAdminRights: Vector[proto.Right.Kind] = Vector(
    proto.Right.Kind.IdentityProviderAdmin(proto.Right.IdentityProviderAdmin())
  )

}
