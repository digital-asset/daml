// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.{StandardJWTPayload, StandardJWTTokenFormat}
import com.daml.ledger.api.v2.admin.package_management_service.*
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.auth.AccessLevel
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.{AuthServiceConfig, CantonConfig}
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

trait PrivilegedTokenAuthIT
    extends ServiceCallAuthTests
    with ErrorsAssertions
    with SubmitAndWaitDummyCommandHelpers {

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
      userId = context.userId,
      party = getPartyId(randomParty),
    )
  }

  protected def toContext(
      payload: StandardJWTPayload,
      secretOverwrite: Option[String] = None,
  ): ServiceCallContext

  protected val privilegedJwtSecret = NonEmptyString.tryCreate(UUID.randomUUID().toString)
  protected val privilegedAdminJwtSecret = NonEmptyString.tryCreate(UUID.randomUUID().toString)

  override val defaultScope: String = ExpectedScope
  // This user does not exist in the user management
  val privilegedEphemeralUser = "PrivilegedUser"

  def privilegedToken: StandardJWTPayload
  def privilegedAdminToken: StandardJWTPayload
  def multipleDiscriminatorsWithExpectedToken: StandardJWTPayload
  def noDiscriminatorToken: StandardJWTPayload
  def wrongDiscriminatorToken: StandardJWTPayload
  def expiredToken: StandardJWTPayload
  def twoDiscriminatorToken: StandardJWTPayload
  def userBasedToken: StandardJWTPayload

  serviceCallName should {
    "allow access to an endpoint requiring admin rights" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a privileged token"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectSuccess(
          serviceCall(toContext(privilegedToken))
        )
      )
    }

    "allow access to an endpoint requiring admin rights with a privileged token at admin access level" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a privileged token at admin access level"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectSuccess(
          serviceCall(toContext(privilegedAdminToken, Some(privilegedAdminJwtSecret.unwrap)))
        )
      )
    }

    "allow access to an endpoint requiring admin rights with user-based authorization" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a regular token"
      ) in { implicit env =>
      import env.*
      expectSuccess {
        serviceCall(toContext(adminToken, Some(jwtSecret.unwrap)))
      }
    }

    "deny access to an endpoint requiring admin rights with user-based authorization" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Ledger API client cannot make a call with a regular token without admin rights"
        )
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectPermissionDenied(
          serviceCall(toContext(userBasedToken, Some(jwtSecret.unwrap)))
        )
      )
    }

    "allow access to an endpoint requiring party rights" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a privileged token"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectSuccess(
          partyCall(toContext(privilegedToken))
        )
      )
    }

    "allow access to an endpoint requiring party rights with user-based authorization" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a regular token"
      ) in { implicit env =>
      import env.*
      expectSuccess {
        partyCall(toContext(userBasedToken, Some(jwtSecret.unwrap)))
      }
    }

    "deny access to an endpoint requiring party rights with user-based authorization" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Ledger API client cannot make a call with a regular token without party rights"
        )
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectPermissionDenied(partyCall(toContext(adminToken, Some(jwtSecret.unwrap))))
      )
    }

    "deny access to an endpoint requiring party rights with a privileged token at admin access level" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Ledger API client cannot make a call requiring party rights with a privileged token at admin access level"
        )
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectPermissionDenied(
          partyCall(toContext(privilegedAdminToken, Some(privilegedAdminJwtSecret.unwrap)))
        )
      )
    }

    "allow access to an endpoint with the token with multiple scopes/audiences which is matching expected scope/audience" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a privileged token with intended scope/audience"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectSuccess {
          serviceCall(toContext(multipleDiscriminatorsWithExpectedToken))
        }
      )
    }

    "allow access to an endpoint with the token which is matching intended scope/audience and additional audience/scope defined" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a privileged token with intended scope/audience"
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule)(
        expectSuccess(serviceCall(toContext(twoDiscriminatorToken)))
      )
    }

    "deny access with no intended scope/audience" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat =
        "Ledger API client cannot make a call with a privileged token with no intended scope/audience"
      )
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(toContext(noDiscriminatorToken)))
      }
    }

    "deny access with wrong intended scope/audience" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat =
        "Ledger API client cannot make a call with a privileged token with wrong intended scope/audience"
      )
    ) in { implicit env =>
      import env.*
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        expectUnauthenticated(serviceCall(toContext(wrongDiscriminatorToken)))
      }
    }

    "deny calls with an expired privileged token" taggedAs adminSecurityAsset.setAttack(
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

}

class PrivilegedScopeTokenAuthIT extends PrivilegedTokenAuthIT {
  registerPlugin(ExpectedScopeOverrideConfig(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  protected def toContext(
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

  private val privilegedScope: String = "Privileged-Scope"
  private val privilegedAdminScope: String = "Privileged-Admin-Scope"

  val privilegedToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = privilegedEphemeralUser,
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Scope,
    audiences = List.empty,
    scope = Some(privilegedScope),
  )

  val privilegedAdminToken: StandardJWTPayload =
    privilegedToken.copy(scope = Some(privilegedAdminScope))

  val multipleDiscriminatorsWithExpectedToken: StandardJWTPayload =
    privilegedToken.copy(scope = Some(s"$privilegedScope additionalScope"))

  val noDiscriminatorToken: StandardJWTPayload =
    privilegedToken.copy(scope = None)

  val wrongDiscriminatorToken: StandardJWTPayload =
    privilegedToken.copy(scope = Some("scope1 scope2"))

  val expiredToken: StandardJWTPayload =
    privilegedToken.copy(exp = Some(Instant.now().plusNanos(Duration.ofDays(-1).toNanos)))

  val twoDiscriminatorToken: StandardJWTPayload =
    privilegedToken.copy(audiences = List("aud1", "aud2"))

  val userBasedToken: StandardJWTPayload = standardToken(randomPartyActUser)

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
              ),
            AuthServiceConfig
              .UnsafeJwtHmac256(
                secret = privilegedJwtSecret,
                targetAudience = None,
                targetScope = Some(privilegedScope),
                privileged = true,
              ),
            AuthServiceConfig
              .UnsafeJwtHmac256(
                secret = privilegedAdminJwtSecret,
                targetAudience = None,
                targetScope = Some(privilegedAdminScope),
                privileged = true,
                accessLevel = AccessLevel.Admin,
              ),
          )
        )
      }(config)
  }
}

class PrivilegedAudienceTokenAuthIT extends PrivilegedTokenAuthIT {
  registerPlugin(ExpectedAudienceOverrideConfig(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  protected def toContext(
      payload: StandardJWTPayload,
      secretOverwrite: Option[String] = None,
  ): ServiceCallContext = ServiceCallContext(
    token = Some(
      toHeader(
        payload = payload,
        enforceFormat = Some(StandardJWTTokenFormat.Audience),
        secret = secretOverwrite.getOrElse(privilegedJwtSecret.unwrap),
      )
    )
  )

  override val adminToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = participantAdmin,
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Audience,
    audiences = List(ExpectedAudience),
    scope = None,
  )

  private val defaultAudience: String = ExpectedAudience
  private val privilegedAudience: String = "Privileged-Audience"
  private val privilegedAdminAudience: String = "Privileged-Admin-Audience"

  val privilegedToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = privilegedEphemeralUser,
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Audience,
    audiences = List(privilegedAudience),
    scope = None,
  )

  val privilegedAdminToken: StandardJWTPayload =
    privilegedToken.copy(audiences = List(privilegedAdminAudience))

  val multipleDiscriminatorsWithExpectedToken: StandardJWTPayload =
    privilegedToken.copy(audiences = List(privilegedAudience, "AdditionalAudience"))

  val noDiscriminatorToken: StandardJWTPayload =
    privilegedToken.copy(audiences = List.empty)

  val wrongDiscriminatorToken: StandardJWTPayload =
    privilegedToken.copy(audiences = List("audience1", "audience2"))

  val expiredToken: StandardJWTPayload =
    privilegedToken.copy(exp = Some(Instant.now().plusNanos(Duration.ofDays(-1).toNanos)))

  val twoDiscriminatorToken: StandardJWTPayload =
    privilegedToken.copy(scope = Some("scope1 scope2"))

  val userBasedToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = randomPartyActUser,
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Audience,
    audiences = List(defaultAudience),
    scope = None,
  )

  //  plugin to override the configuration and use authorization with audience
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
                targetAudience = Some(defaultAudience),
                targetScope = None,
              ),
            AuthServiceConfig
              .UnsafeJwtHmac256(
                secret = privilegedJwtSecret,
                targetAudience = Some(privilegedAudience),
                targetScope = None,
                privileged = true,
              ),
            AuthServiceConfig
              .UnsafeJwtHmac256(
                secret = privilegedAdminJwtSecret,
                targetAudience = Some(privilegedAdminAudience),
                targetScope = None,
                privileged = true,
                accessLevel = AccessLevel.Admin,
              ),
          )
        )
      }(config)
  }
}
