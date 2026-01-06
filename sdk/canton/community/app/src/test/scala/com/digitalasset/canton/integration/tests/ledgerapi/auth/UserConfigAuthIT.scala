// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.{StandardJWTPayload, StandardJWTTokenFormat}
import com.daml.ledger.api.v2.admin.package_management_service.*
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.auth.{AuthorizedUser, CantonAdminToken}
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.{AuthServiceConfig, CantonConfig}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthServiceJWTSuppressionRule
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitAndWaitDummyCommandHelpers
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentSetupPlugin,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import monocle.macros.syntax.lens.*

import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.Future

class UserConfigAuthIT
    extends ServiceCallAuthTests
    with ErrorsAssertions
    with SubmitAndWaitDummyCommandHelpers {

  registerPlugin(ExpectedScopeOverrideConfig(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def prerequisiteParties: List[String] = List(randomParty)

  override def serviceCallName: String =
    "Any service call with target scope based token authorization"

  protected def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(PackageManagementServiceGrpc.stub(channel), context.token)
      .listKnownPackages(ListKnownPackagesRequest())

  def partyCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    submitAndWait(
      token = context.token,
      userId = "",
      party = getPartyId(randomParty),
    )
  }

  private def toContext(
      payload: StandardJWTPayload,
      secretOverwrite: Option[String] = None,
  ): ServiceCallContext = ServiceCallContext(
    token = Some(
      toHeader(
        payload = payload,
        enforceFormat = Some(StandardJWTTokenFormat.Scope),
        secret = secretOverwrite.getOrElse(privilegedJwtSecret.unwrap),
      )
    )
  )

  private val cantonAdminToken = CantonAdminToken.create(new SymbolicPureCrypto())

  private val configuredUser = randomUserId()
  private val otherUser = randomUserId()

  override val defaultScope: String = ExpectedScope

  private val privilegedJwtSecret = NonEmptyString.tryCreate(UUID.randomUUID().toString)
  private val privilegedScope: String = "Privileged-Scope"

  val adminTokenStandardJWT: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = participantAdmin,
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Scope,
    audiences = List.empty,
    scope = Some(defaultScope),
  )

  val configuredUserToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = configuredUser,
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Scope,
    audiences = List.empty,
    scope = Some(privilegedScope),
  )

  val otherUserToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = otherUser,
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Scope,
    audiences = List.empty,
    scope = Some(privilegedScope),
  )

  val multipleScopesWithExpectedToken: StandardJWTPayload =
    configuredUserToken.copy(scope = Some(s"$privilegedScope additionalScope"))

  val noScopeToken: StandardJWTPayload =
    configuredUserToken.copy(scope = None)

  val wrongScopeToken: StandardJWTPayload =
    configuredUserToken.copy(scope = Some("scope1 scope2"))

  val expiredToken: StandardJWTPayload =
    configuredUserToken.copy(exp = Some(Instant.now().plusNanos(Duration.ofDays(-1).toNanos)))

  val audienceToken: StandardJWTPayload = configuredUserToken.copy(audiences = List("aud1", "aud2"))

  serviceCallName should {
    "allow access to a configured endpoint requiring admin rights with canton admin token" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a canton admin token"
      ) in { implicit env =>
      import env.*
      expectSuccess {
        serviceCall(ServiceCallContext(token = Some(cantonAdminToken.secret)))
      }
    }

    "allow access to a configured endpoint requiring admin rights" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a token for configured user"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectSuccess(
          serviceCall(toContext(configuredUserToken))
        )
      )
    }

    "deny access to a configured endpoint requiring admin rights" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Ledger API client cannot make a call with a token for a user not in configuration"
        )
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectUnauthenticated(
          serviceCall(toContext(otherUserToken))
        )
      )
    }

    "allow access to an endpoint with the token with multiple scopes which is matching expected scope" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a privileged token with intended scope"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectSuccess {
          serviceCall(toContext(multipleScopesWithExpectedToken))
        }
      )
    }

    "allow access to an endpoint with the token which is matching intended scope and audiences defined" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a privileged token with intended scope"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectSuccess(serviceCall(toContext(audienceToken)))
      )
    }

    "deny access with no intended scope" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat =
        "Ledger API client cannot make a call with a privileged token with no intended scope"
      )
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(toContext(noScopeToken)))
      }
    }

    "deny access with wrong intended scope" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat =
        "Ledger API client cannot make a call with a privileged token with wrong intended scope"
      )
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(toContext(wrongScopeToken)))
      }
    }

    "deny calls with an expired token" taggedAs adminSecurityAsset.setAttack(
      attackUnauthenticated(threat = "Present an expired privileged token")
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

  private val authorizedUser = AuthorizedUser(
    userId = configuredUser,
    allowedServices = List(
      "com.daml.ledger.api.v2.admin.PackageManagementService"
    ),
  )

  //  plugin to override the configuration and use authorization with scope
  case class ExpectedScopeOverrideConfig(
      protected val loggerFactory: NamedLoggerFactory
  ) extends EnvironmentSetupPlugin {
    override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
      ConfigTransforms.updateParticipantConfig("participant1") {
        _.focus(_.ledgerApi.adminTokenConfig.fixedAdminToken)
          .replace(Some(cantonAdminToken.secret))
          .focus(_.ledgerApi.authServices)
          .replace(
            Seq(
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = jwtSecret,
                  targetAudience = None,
                  targetScope = Some(defaultScope),
                ),
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = privilegedJwtSecret,
                  targetAudience = None,
                  targetScope = Some(privilegedScope),
                  users = List(authorizedUser),
                ),
            )
          )
          .focus(_.ledgerApi.adminTokenConfig.adminClaim)
          .replace(true)
      }(config)
  }
}
