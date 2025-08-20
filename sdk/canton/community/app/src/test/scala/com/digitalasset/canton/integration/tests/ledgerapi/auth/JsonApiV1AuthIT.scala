// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.{AuthServiceJWTCodec, Jwt, StandardJWTPayload, StandardJWTTokenFormat}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.config.{AuthServiceConfig, CantonConfig, DbConfig}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.HttpServiceTestFixtureData
import com.digitalasset.canton.integration.tests.jsonapi.{
  HttpServiceTestFixture,
  HttpServiceUserFixture,
  HttpTestFuns,
}
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.{
  AuthInterceptorSuppressionRule,
  AuthServiceJWTSuppressionRule,
}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.integration.tests.ledgerapi.services.TestCommands
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentSetupPlugin}
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressionRule}
import monocle.macros.syntax.lens.*
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes, Uri}
import org.slf4j.event.Level

import java.io.File
import java.time.{Duration, Instant}
import scala.concurrent.Future

class JsonApiV1AuthIT
    extends CantonFixture
    with SecurityTags
    with TestCommands
    with HttpTestFuns
    with HttpServiceUserFixture.UserToken
    with ErrorsAssertions {

  registerPlugin(ExpectedScopeOverrideConfig(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override protected def packageFiles: List[File] =
    List(super.darFile)

  override def serviceCallName: String =
    "JSON API"

  private def expect(code: StatusCode, forbiddenMessage: Option[String] = None)(
      call: => Future[(StatusCode, String)]
  ) =
    for {
      (actualCode, actualResponse) <- call
    } yield {
      forbiddenMessage.fold(succeed)(actualResponse should not include _)
      actualCode should be(code)
    }

  private def publicCall(
      fixture: HttpServiceTestFixtureData,
      context: ServiceCallContext,
  ): Future[(StatusCode, String)] = {

    val headers = HttpServiceTestFixture.authorizationHeader(Jwt(context.token.getOrElse("")))
    fixture.getRequestString(Uri.Path("/v1/parties"), headers)

  }

  def callRequiringPartyClaims(
      fixture: HttpServiceTestFixtureData,
      context: ServiceCallContext,
  ): Future[(StatusCode, String)] = {
    val headers = HttpServiceTestFixture.authorizationHeader(Jwt(context.token.getOrElse("")))
    fixture.getRequestString(Uri.Path("/v1/query"), headers)
  }

  private def toScopeContext(
      payload: StandardJWTPayload
  ): ServiceCallContext = ServiceCallContext(
    token = Some(
      toHeader(
        payload = payload,
        enforceFormat = Some(StandardJWTTokenFormat.Scope),
        secret = jwtSecret.unwrap,
      )
    )
  )

  private val fallbackToken = CantonAdminToken.create(new SymbolicPureCrypto())

  override val defaultScope: String = AuthServiceJWTCodec.scopeLedgerApiFull

  case class Tokens(
      user: StandardJWTPayload,
      admin: StandardJWTPayload,
      additionalDiscriminator: StandardJWTPayload,
      noDiscriminator: StandardJWTPayload,
      wrongDiscriminator: StandardJWTPayload,
      mixedDiscriminators: StandardJWTPayload,
      expired: StandardJWTPayload,
      unknownUser: StandardJWTPayload,
  )

  val scopeBaseToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = participantAdmin,
    exp = None,
    format = StandardJWTTokenFormat.Scope,
    audiences = List.empty,
    scope = Some(AuthServiceJWTCodec.scopeLedgerApiFull),
  )
  val scope: Tokens = Tokens(
    user = scopeBaseToken.copy(userId = randomPartyActUser),
    admin = scopeBaseToken,
    additionalDiscriminator = scopeBaseToken.copy(scope = Some(s"$defaultScope additionalScope")),
    noDiscriminator = scopeBaseToken.copy(scope = None),
    wrongDiscriminator = scopeBaseToken.copy(scope = Some("scope1 scope2")),
    mixedDiscriminators = scopeBaseToken
      .copy(audiences = List(ExpectedAudience), scope = Some(s"$defaultScope additionalScope")),
    expired = scopeBaseToken.copy(exp = Some(Instant.now().plusNanos(Duration.ofDays(-1).toNanos))),
    unknownUser = scopeBaseToken.copy(userId = "unknown_user"),
  )

  serviceCallName should {
    "allow access to an endpoint requiring admin rights with fall-back authorization" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a canton admin token"
      ) in httpTestFixture { fixture =>
      expect(StatusCodes.OK) {
        publicCall(fixture, ServiceCallContext(token = Some(fallbackToken.secret)))
      }
    }

    "allow access to an endpoint requiring admin rights with admin token" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call requiring admin rights with an admin token"
      ) in httpTestFixture { fixture =>
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        for {
          _ <- expect(StatusCodes.OK)(
            publicCall(fixture, toScopeContext(scope.admin))
          )
        } yield ()
      }
    }

    "deny access to an endpoint requiring party rights with admin token" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Ledger API client cannot make a call requiring party rights with an admin token"
        )
      ) in httpTestFixture { fixture =>
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        for {
          _ <- expect(StatusCodes.Unauthorized)(
            callRequiringPartyClaims(fixture, toScopeContext(scope.admin))
          )
        } yield ()
      }
    }

    "deny access to an endpoint requiring admin rights with user token" taggedAs securityAsset
      .setAttack(
        attackPermissionDenied(threat =
          "Ledger API client cannot make a call requiring admin rights with a user token"
        )
      ) in httpTestFixture { fixture =>
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        for {
          _ <- expect(StatusCodes.Unauthorized)(
            publicCall(fixture, toScopeContext(scope.user))
          )
        } yield ()
      }
    // There is no concept of user token for privilegedScp and privilegedAud jwt
    }

    "allow access to an endpoint requiring party rights with user token" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call requiring party rights with a user token"
      ) in httpTestFixture { fixture =>
      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.DEBUG))(
        for {
          _ <- expect(StatusCodes.OK, Some("UNAUTHENTICATED"))(
            callRequiringPartyClaims(fixture, toScopeContext(scope.user))
          )
        } yield (),
        entries => {
          forEvery(entries)(
            _.message should not include "failed with UNAUTHENTICATED/An error occurred"
          )
        },
      )
    }

//    "allow access to an endpoint with a token that mixes expected audiences/scopes" taggedAs securityAsset
//      .setHappyCase(
//        "Ledger API client can make a call with a token with multiple expected audiences/scopes"
//      ) in httpTestFixture { fixture =>
//      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
//        for {
//          _ <- expect(StatusCodes.OK)(
//            publicCall(fixture, toScopeContext(scope.mixedDiscriminators))
//          )
//        } yield ()
//      }
//    }

    "deny access to an endpoint with an expired token" taggedAs adminSecurityAsset.setAttack(
      attackUnauthenticated(threat = "Present an expired token")
    ) in httpTestFixture { fixture =>
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {

        for {
          _ <- expect(StatusCodes.InternalServerError)(
            publicCall(fixture, toScopeContext(scope.expired))
          )
        } yield ()
      }
    }

    "deny access to an endpoint with an token for an unknown user" taggedAs adminSecurityAsset
      .setAttack(
        attackUnauthenticated(threat = "Present a token for unknown user")
      ) in httpTestFixture { fixture =>
      loggerFactory.suppress(AuthServiceJWTSuppressionRule || AuthInterceptorSuppressionRule) {
        for {
          _ <- expect(StatusCodes.Unauthorized)(
            publicCall(fixture, toScopeContext(scope.unknownUser))
          )
        } yield ()
        // There is no concept of user token for privilegedScp and privilegedAud jwt
      }
    }

    "deny unauthenticated access" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat = "Do not present token")
    ) in httpTestFixture { fixture =>
      expect(StatusCodes.Unauthorized)(
        publicCall(fixture, noToken)
      )
    }
  }

  //  plugin to override the configuration and use authorization with scope
  case class ExpectedScopeOverrideConfig(
      protected val loggerFactory: NamedLoggerFactory
  ) extends EnvironmentSetupPlugin {
    override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
      ConfigTransforms.updateParticipantConfig("participant1") {
        _.focus(_.ledgerApi.authServices)
          .replace(
            Seq(
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = jwtSecret,
                  targetAudience = None,
                  targetScope = None,
                )
            )
          )
          .focus(_.adminApi.adminTokenConfig.fixedAdminToken)
          .replace(Some(fallbackToken.secret))
      }(config)
  }
}
