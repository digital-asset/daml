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

import java.security.interfaces.RSAPrivateKey
import java.util.UUID
import scala.concurrent.Future

trait AdminServiceCallAuthTests
    extends SecuredServiceCallAuthTests
    with IdentityProviderConfigAuth {

  def denyCallsWithIdpAdmin: Boolean = true
  def denyReadAsAny: Boolean = true

  private val signedIncorrectly = ServiceCallContext(
    Option(toHeader(adminToken, UUID.randomUUID.toString))
  )

  protected def serviceCallWithFreshUser(
      rights: Vector[proto.Right.Kind]
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    createUserByAdmin(UUID.randomUUID().toString, rights = rights.map(proto.Right(_)))
      .flatMap { case (_, context) => serviceCall(context) }
  }

  serviceCallName should {

    "deny calls with an invalid signature when expecting an admin token" taggedAs adminSecurityAsset
      .setAttack(
        attackUnauthenticated(threat = "Present an admin JWT signed by unknown key")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(signedIncorrectly))
      }
    }
    "deny calls with an expired admin token" taggedAs adminSecurityAsset.setAttack(
      attackUnauthenticated(threat = "Present an expired admin JWT")
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(canReadAsAdminExpired))
      }
    }
    "deny calls with a read-only token" taggedAs adminSecurityAsset.setAttack(
      attackPermissionDenied(threat = "Present a read-only user JWT with an unknown party")
    ) in { implicit env =>
      import env.*
      expectPermissionDenied(serviceCall(canReadAsRandomParty))
    }
    "deny calls with a read/write token" taggedAs adminSecurityAsset.setAttack(
      attackPermissionDenied(threat = "Present a read/write user JWT for an unknown party")
    ) in { implicit env =>
      import env.*
      expectPermissionDenied(serviceCall(canActAsRandomParty))
    }
    if (denyReadAsAny) {
      "deny calls with a read-as-any-party token" taggedAs adminSecurityAsset.setAttack(
        attackPermissionDenied(threat = "Present a read-as-any-party user")
      ) in { implicit env =>
        import env.*
        expectPermissionDenied(serviceCall(canReadAsAnyParty))
      }
    }
    "allow calls with explicitly non-expired admin token" taggedAs adminSecurityAsset
      .setHappyCase(
        "Ledger API client can make a call with explicitly non-expired admin JWT"
      ) in { implicit env =>
      import env.*
      expectSuccess(serviceCall(canReadAsAdminExpiresInAnHour))
    }
    "allow calls with admin token without expiration" taggedAs adminSecurityAsset
      .setHappyCase(
        "Ledger API client can make a call with admin JWT without expiration"
      ) in { implicit env =>
      import env.*
      expectSuccess(serviceCall(canBeAnAdmin))
    }
    "allow calls for 'participant_admin' user without expiration" taggedAs adminSecurityAsset
      .setHappyCase(
        "Ledger API client can make a call with 'participant_admin' JWT without expiration"
      ) in { implicit env =>
      import env.*
      expectSuccess(serviceCall(canBeAnAdmin))
    }
    "allow calls with freshly created admin user" taggedAs adminSecurityAsset.setHappyCase(
      "Ledger API client can make a call with freshly created admin user"
    ) in { implicit env =>
      import env.*
      expectSuccess(
        serviceCallWithFreshUser(
          Vector(proto.Right.Kind.ParticipantAdmin(proto.Right.ParticipantAdmin()))
        )
      )
    }
    "deny calls with freshly created non-admin user" taggedAs adminSecurityAsset.setAttack(
      attackPermissionDenied(threat = "Present a user JWT for a freshly created non-admin user")
    ) in { implicit env =>
      import env.*
      expectPermissionDenied(serviceCallWithFreshUser(Vector.empty))
    }
    "deny calls with user token for 'unknown_user' without expiration" taggedAs adminSecurityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present a user JWT for 'unknown_user' without expiration")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthInterceptorSuppressionRule) {
        expectPermissionDenied(serviceCall(canReadAsUnknownUserStandardJWT))
      }
    }
    "deny calls with user token for '!!invalid_user!!' without expiration" taggedAs adminSecurityAsset
      .setAttack(
        attackInvalidArgument(threat =
          "Present a JWT with unparseable '!!invalid_user!!' without expiration"
        )
      ) in { implicit env =>
      import env.*
      expectInvalidArgument(serviceCall(canReadAsInvalidUserStandardJWT))
    }

    if (denyCallsWithIdpAdmin) {
      "deny calls with an IDP Admin user" taggedAs adminSecurityAsset
        .setAttack(
          attackPermissionDenied("Present an IDP admin token")
        ) in { implicit env =>
        import env.*
        loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
          expectPermissionDenied {
            for {
              idpConfig <- createConfig(canBeAnAdmin)
              tokenIssuer = Some(idpConfig.issuer)
              _ <- serviceCallWithIDPUser(
                userId = UUID.randomUUID().toString,
                rights = Vector(
                  proto.Right.Kind.IdentityProviderAdmin(proto.Right.IdentityProviderAdmin())
                ),
                identityProviderId = idpConfig.identityProviderId,
                tokenIssuer = tokenIssuer,
                privateKey = Some(key1.privateKey),
                keyId = key1.id,
              )
            } yield ()
          }
        }
      }
    }
  }

  def serviceCallWithIDPUser(
      userId: String,
      rights: Vector[proto.Right.Kind],
      identityProviderId: String,
      tokenIssuer: Option[String],
      privateKey: Option[RSAPrivateKey] = None,
      keyId: String = "",
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    createUserByAdminRSA(
      userId = userId,
      identityProviderId = identityProviderId,
      tokenIssuer = tokenIssuer,
      rights = rights.map(proto.Right(_)),
      privateKey = privateKey.getOrElse(privateKeyParticipantAdmin),
      keyId = keyId,
    ).flatMap { case (_, context) =>
      serviceCall(context)
    }
  }

}
