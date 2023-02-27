// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.auth.StandardJWTTokenFormat
import com.daml.ledger.api.v1.admin.identity_provider_config_service.{
  CreateIdentityProviderConfigResponse,
  IdentityProviderConfig,
  UpdateIdentityProviderConfigRequest,
}
import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import com.daml.platform.sandbox.TestJwtVerifierLoader
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import com.google.protobuf.field_mask.FieldMask

import java.util.UUID
import scala.concurrent.Future

trait AdminOrIDPAdminServiceCallAuthTests
    extends AdminServiceCallAuthTests
    with IdentityProviderConfigAuth {

  private def serviceCallWithIDPUser(
      rights: Vector[proto.Right.Kind],
      identityProviderId: String,
      tokenIssuer: Option[String],
      secret: Option[String] = None,
  ): Future[Any] =
    createUserByAdmin(
      userId = UUID.randomUUID().toString,
      identityProviderId = identityProviderId,
      tokenIssuer = tokenIssuer,
      rights = rights.map(proto.Right(_)),
      secret = secret.orElse(Some(TestJwtVerifierLoader.secret1)),
    ).flatMap { case (_, context) =>
      serviceCall(context)
    }

  it should "allow calls with freshly created IDP Admin user within IDP" taggedAs adminSecurityAsset
    .setHappyCase(
      "Ledger API client can make a call with freshly created IDP admin user"
    ) in {
    expectSuccess {
      for {
        response <- createConfig(canReadAsAdminStandardJWT)
        identityProviderConfig = response.identityProviderConfig
          .getOrElse(sys.error("Failed to create idp config"))
        tokenIssuer = Some(identityProviderConfig.issuer)
        _ <- serviceCallWithIDPUser(idpAdminRights, toIdentityProviderId(response), tokenIssuer)
      } yield ()
    }
  }

  private def audienceBasedToken(
      userId: String,
      identityProviderConfig: IdentityProviderConfig,
      audience: List[String],
  ) =
    Some(
      toHeader(
        standardToken(userId, issuer = Some(identityProviderConfig.issuer))
          .copy(
            audiences = audience,
            format = StandardJWTTokenFormat.ParticipantId,
          ),
        secret = TestJwtVerifierLoader.secret1,
        audienceBasedToken = true,
      )
    )

  it should "allow calls with freshly created IDP Admin user within IDP and intended audience" taggedAs adminSecurityAsset
    .setHappyCase(
      "Ledger API client can make a call with freshly created IDP admin user"
    ) in {
    expectSuccess {
      val userId = UUID.randomUUID().toString
      val identityProviderConfig = IdentityProviderConfig(
        identityProviderId = UUID.randomUUID().toString,
        isDeactivated = false,
        issuer = UUID.randomUUID().toString,
        jwksUrl = TestJwtVerifierLoader.jwksUrl1.value,
        audience = UUID.randomUUID().toString,
      )
      for {
        _ <- createConfig(canReadAsAdminStandardJWT, identityProviderConfig)
        (_, _) <- createUserByAdmin(
          userId = userId,
          rights = idpAdminRights.map(proto.Right(_)),
          identityProviderId = identityProviderConfig.identityProviderId,
        )
        token = audienceBasedToken(
          userId,
          identityProviderConfig,
          List(identityProviderConfig.audience),
        )
        context = ServiceCallContext(token, true, identityProviderConfig.identityProviderId)
        _ <- serviceCall(context)
      } yield ()
    }
  }

  it should "deny calls with freshly created IDP Admin user within IDP and missing intended audience" taggedAs adminSecurityAsset
    .setAttack(attackPermissionDenied(threat = "Present a JWT with a missing audience")) in {
    expectUnauthenticated {
      val userId = UUID.randomUUID().toString
      val identityProviderConfig = IdentityProviderConfig(
        identityProviderId = UUID.randomUUID().toString,
        isDeactivated = false,
        issuer = UUID.randomUUID().toString,
        jwksUrl = TestJwtVerifierLoader.jwksUrl1.value,
        audience = UUID.randomUUID().toString,
      )
      for {
        _ <- createConfig(canReadAsAdminStandardJWT, identityProviderConfig)
        (_, _) <- createUserByAdmin(
          userId = userId,
          rights = idpAdminRights.map(proto.Right(_)),
          identityProviderId = identityProviderConfig.identityProviderId,
        )
        token = audienceBasedToken(userId, identityProviderConfig, audience = List.empty)
        context = ServiceCallContext(token, true, identityProviderConfig.identityProviderId)
        _ <- serviceCall(context)
      } yield ()
    }
  }

  it should "deny calls with freshly created IDP Admin user with missing issuer" taggedAs adminSecurityAsset
    .setAttack(attackPermissionDenied(threat = "Present a JWT with a missing issuer")) in {
    expectUnauthenticated {
      for {
        response <- createConfig(canReadAsAdminStandardJWT)
        _ <- serviceCallWithIDPUser(idpAdminRights, toIdentityProviderId(response), None)
      } yield ()
    }
  }

  it should "deny calls with user token within IDP which is deactivated" taggedAs adminSecurityAsset
    .setAttack(attackPermissionDenied(threat = "Present a JWT of inactive IDP")) in {
    expectPermissionDenied {
      val idpId = UUID.randomUUID().toString
      val tokenIssuer = UUID.randomUUID().toString
      val identityProviderConfig = IdentityProviderConfig(
        identityProviderId = idpId,
        isDeactivated = false,
        issuer = tokenIssuer,
        jwksUrl = TestJwtVerifierLoader.jwksUrl1.value,
      )
      for {
        response1 <- createConfig(canReadAsAdminStandardJWT, identityProviderConfig)
        (_, context) <- createUserByAdmin(
          userId = UUID.randomUUID().toString,
          identityProviderId = toIdentityProviderId(response1),
          tokenIssuer = Some(tokenIssuer),
          rights = idpAdminRights.map(proto.Right(_)),
        )
        _ <- idpStub(context).updateIdentityProviderConfig(
          UpdateIdentityProviderConfigRequest(
            identityProviderConfig =
              response1.identityProviderConfig.map(_.copy(isDeactivated = true)),
            updateMask = Some(FieldMask(scala.Seq("is_deactivated"))),
          )
        )
        _ <- serviceCall(context)
      } yield ()
    }
  }

  it should "deny calls with freshly created user within IDP with missing IDP Admin right" taggedAs adminSecurityAsset
    .setAttack(attackPermissionDenied(threat = "Act without IDP Admin permission")) in {
    expectPermissionDenied {
      for {
        response <- createConfig(canReadAsAdminStandardJWT)
        identityProviderConfig = response.identityProviderConfig
          .getOrElse(sys.error("Failed to create idp config"))
        tokenIssuer = Some(identityProviderConfig.issuer)
        _ <- serviceCallWithIDPUser(Vector(), toIdentityProviderId(response), tokenIssuer)
      } yield ()
    }
  }

  it should "deny calls with freshly created user within IDP1 while acting on IDP2" taggedAs adminSecurityAsset
    .setAttack(attackPermissionDenied(threat = "Act within non-authorized IDP")) in {
    expectPermissionDenied {
      for {
        response1 <- createConfig(canReadAsAdminStandardJWT)
        response2 <- createConfig(canReadAsAdminStandardJWT)
        identityProviderConfig1 = response1.identityProviderConfig
          .getOrElse(sys.error("Failed to create idp config"))
        tokenIssuer1 = Some(identityProviderConfig1.issuer)
        _ <- createUserByAdmin(
          userId = UUID.randomUUID().toString,
          identityProviderId = toIdentityProviderId(response1),
          tokenIssuer = tokenIssuer1,
          rights = idpAdminRights.map(proto.Right(_)),
        ).flatMap { case (_, context) =>
          serviceCall(context.copy(identityProviderId = toIdentityProviderId(response2)))
        }
      } yield ()
    }
  }

  it should "deny calls with user token signed by secret of another IDP" taggedAs adminSecurityAsset
    .setAttack(attackPermissionDenied(threat = "Present a JWT signed by wrong secret")) in {
    expectUnauthenticated {
      val idpId = UUID.randomUUID().toString
      val tokenIssuer = UUID.randomUUID().toString
      val identityProviderConfig = IdentityProviderConfig(
        identityProviderId = idpId,
        isDeactivated = false,
        issuer = tokenIssuer,
        jwksUrl = TestJwtVerifierLoader.jwksUrl1.value,
      )
      for {
        _ <- createConfig(canReadAsAdminStandardJWT, identityProviderConfig)
        _ <- serviceCallWithIDPUser(
          rights = idpAdminRights,
          identityProviderId = idpId,
          tokenIssuer = Some(tokenIssuer),
          secret = Some(TestJwtVerifierLoader.secret2),
        )
      } yield ()
    }
  }

  it should "deny calls if Jwks url is unreachable" taggedAs adminSecurityAsset
    .setAttack(
      attackPermissionDenied(threat =
        "Present a JWT which cannot be validated as Jwks URL is unreachable"
      )
    ) in {
    expectUnauthenticated {
      val suffix = UUID.randomUUID().toString
      val idpConfig = IdentityProviderConfig(
        identityProviderId = "idp-id-" + suffix,
        isDeactivated = false,
        issuer = "issuer-" + suffix,
        jwksUrl =
          TestJwtVerifierLoader.jwksUrl3.value, // TestJwtVerifierLoader.jwksUrl3 returns failure
      )
      for {
        response <- createConfig(canReadAsAdminStandardJWT, idpConfig)
        identityProviderConfig = response.identityProviderConfig
          .getOrElse(sys.error("Failed to create idp config"))
        tokenIssuer = Some(identityProviderConfig.issuer)
        _ <- serviceCallWithIDPUser(idpAdminRights, toIdentityProviderId(response), tokenIssuer)
      } yield ()
    }
  }

  def toIdentityProviderId(response: CreateIdentityProviderConfigResponse): String = {
    val identityProviderConfig = response.identityProviderConfig
      .getOrElse(sys.error("Failed to create idp config"))
    identityProviderConfig.identityProviderId
  }

  def idpAdminRights: Vector[proto.Right.Kind] = Vector(
    proto.Right.Kind.IdentityProviderAdmin(proto.Right.IdentityProviderAdmin())
  )
}
