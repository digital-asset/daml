// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.{StandardJWTPayload, StandardJWTTokenFormat}
import com.daml.ledger.api.v2.admin.package_management_service.*
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.config.{AuthServiceConfig, CantonConfig, DbConfig}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.{
  AuthInterceptorSuppressionRule,
  AuthServiceJWTSuppressionRule,
}
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentSetupPlugin,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import monocle.macros.syntax.lens.*

import java.time.{Duration, Instant}
import scala.concurrent.Future

class AudienceBasedTokenAuthIT extends ServiceCallAuthTests with ErrorsAssertions {

  registerPlugin(ExpectedAudienceOverrideConfig(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String =
    "Any service call with target audience based token authorization"

  override protected def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(PackageManagementServiceGrpc.stub(channel), context.token)
      .listKnownPackages(ListKnownPackagesRequest())

  private def toContext(payload: StandardJWTPayload): ServiceCallContext = ServiceCallContext(
    token = Some(
      toHeader(
        payload = payload,
        enforceFormat = Some(StandardJWTTokenFormat.Audience),
      )
    )
  )

  val expectedAudienceToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = participantAdmin,
    exp = None,
    format = StandardJWTTokenFormat.Audience,
    audiences = List(ExpectedAudience),
    scope = None,
  )

  override val adminToken: StandardJWTPayload = expectedAudienceToken

  val multipleAudienceWithExpectedToken: StandardJWTPayload =
    expectedAudienceToken.copy(audiences = List(ExpectedAudience, "additionalAud"))

  val noAudienceToken: StandardJWTPayload =
    expectedAudienceToken.copy(audiences = List())

  val wrongAudienceToken: StandardJWTPayload =
    expectedAudienceToken.copy(audiences = List("aud1", "aud2"))

  val expiredToken: StandardJWTPayload =
    expectedAudienceToken.copy(exp = Some(Instant.now().plusNanos(Duration.ofDays(-1).toNanos)))

  val unknownUserToken: StandardJWTPayload =
    expectedAudienceToken.copy(userId = "unknown_user")

  val scopeToken: StandardJWTPayload = expectedAudienceToken.copy(scope = Some("someScope"))

  serviceCallName should {
    "allow access to an endpoint with the token which is matching intended audience" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a JWT with intended audience"
      ) in { implicit env =>
      import env.*
      expectSuccess {
        serviceCall(toContext(expectedAudienceToken))
      }
    }

    "allow access to an endpoint with the token with multiple audiences which is matching expected audience" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a JWT with intended audience"
      ) in { implicit env =>
      import env.*
      expectSuccess {
        serviceCall(toContext(multipleAudienceWithExpectedToken))
      }
    }

    "allow access to an endpoint with the token which is matching intended audience and scope defined" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a JWT with intended audience"
      ) in { implicit env =>
      import env.*
      expectSuccess(serviceCall(toContext(scopeToken)))
    }

    "deny access with no intended audience" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat =
        "Ledger API client cannot make a call with a JWT with no intended audience"
      )
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(toContext(noAudienceToken)))
      }
    }

    "deny access with wrong intended audience" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat =
        "Ledger API client cannot make a call with a JWT with wrong intended audience"
      )
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(toContext(wrongAudienceToken)))
      }
    }

    "deny calls with user token for 'unknown_user' without expiration" taggedAs adminSecurityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present a user JWT for 'unknown_user' without expiration")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthInterceptorSuppressionRule) {
        expectPermissionDenied(serviceCall(toContext(unknownUserToken)))
      }
    }

    "deny calls with an expired admin token" taggedAs adminSecurityAsset.setAttack(
      attackUnauthenticated(threat = "Present an expired admin JWT")
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(toContext(expiredToken)))
      }
    }

    "deny unauthenticated access" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat = "Do not present JWT")
    ) in { implicit env =>
      import env.*
      expectUnauthenticated(serviceCall(noToken))
    }

    "return invalid argument for scope token" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat = "Present a custom JWT")
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(
          serviceCall(ServiceCallContext(Option(toHeader(standardToken(participantAdmin)))))
        )
      }
    }
  }

  //  plugin to override the configuration and use authorization with audiences
  case class ExpectedAudienceOverrideConfig(
      protected val loggerFactory: NamedLoggerFactory
  ) extends EnvironmentSetupPlugin {
    override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
      ConfigTransforms.updateParticipantConfig("participant1") {
        _.focus(_.ledgerApi.authServices).replace(
          Seq(
            AuthServiceConfig
              .UnsafeJwtHmac256(
                secret = jwtSecret,
                targetAudience = Some(ExpectedAudience),
                targetScope = None,
              )
          )
        )
      }(config)
  }
}
