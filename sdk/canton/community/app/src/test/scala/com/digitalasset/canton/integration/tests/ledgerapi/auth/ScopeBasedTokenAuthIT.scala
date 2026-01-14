// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.{StandardJWTPayload, StandardJWTTokenFormat}
import com.daml.ledger.api.v2.admin.package_management_service.*
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.config.{AuthServiceConfig, CantonConfig}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
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

class ScopeBasedTokenAuthIT extends ServiceCallAuthTests with ErrorsAssertions {

  registerPlugin(ExpectedScopeOverrideConfig(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String =
    "Any service call with target scope based token authorization"

  override protected def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(PackageManagementServiceGrpc.stub(channel), context.token)
      .listKnownPackages(ListKnownPackagesRequest())

  private def toContext(payload: StandardJWTPayload): ServiceCallContext = ServiceCallContext(
    token = Some(
      toHeader(
        payload = payload,
        enforceFormat = Some(StandardJWTTokenFormat.Scope),
      )
    )
  )

  override val defaultScope: String = ExpectedScope

  val expectedScopeToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = participantAdmin,
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Scope,
    audiences = List.empty,
    scope = Some(defaultScope),
  )

  val multipleScopesWithExpectedToken: StandardJWTPayload =
    expectedScopeToken.copy(scope = Some(s"$defaultScope additionalScope"))

  val noScopeToken: StandardJWTPayload =
    expectedScopeToken.copy(scope = None)

  val wrongScopeToken: StandardJWTPayload =
    expectedScopeToken.copy(scope = Some("scope1 scope2"))

  val expiredToken: StandardJWTPayload =
    expectedScopeToken.copy(exp = Some(Instant.now().plusNanos(Duration.ofDays(-1).toNanos)))

  val unknownUserToken: StandardJWTPayload =
    expectedScopeToken.copy(userId = "unknown_user")

  val audienceToken: StandardJWTPayload = expectedScopeToken.copy(audiences = List("aud1", "aud2"))

  serviceCallName should {
    "allow access to an endpoint with the token which is matching intended scope" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a JWT with intended scope"
      ) in { implicit env =>
      import env.*
      expectSuccess {
        serviceCall(toContext(expectedScopeToken))
      }
    }

    "allow access to an endpoint with the token with multiple scopes which is matching expected scope" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a JWT with intended scope"
      ) in { implicit env =>
      import env.*
      expectSuccess {
        serviceCall(toContext(multipleScopesWithExpectedToken))
      }
    }

    "allow access to an endpoint with the token which is matching intended scope and audiences defined" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a JWT with intended scope"
      ) in { implicit env =>
      import env.*
      expectSuccess(serviceCall(toContext(audienceToken)))
    }

    "deny access with no intended scope" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat =
        "Ledger API client cannot make a call with a JWT with no intended scope"
      )
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(toContext(noScopeToken)))
      }
    }

    "deny access with wrong intended scope" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat =
        "Ledger API client cannot make a call with a JWT with wrong intended scope"
      )
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(toContext(wrongScopeToken)))
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
  }

  //  plugin to override the configuration and use authorization with scope
  case class ExpectedScopeOverrideConfig(
      protected val loggerFactory: NamedLoggerFactory
  ) extends EnvironmentSetupPlugin {
    override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
      ConfigTransforms.updateParticipantConfig("participant1") {
        _.focus(_.ledgerApi.authServices).replace(
          Seq(
            AuthServiceConfig
              .UnsafeJwtHmac256(
                secret = jwtSecret,
                targetAudience = None,
                targetScope = Some(defaultScope),
              )
          )
        )
      }(config)
  }
}
