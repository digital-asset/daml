// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.{Jwt, StandardJWTPayload, StandardJWTTokenFormat}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.Attack
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.config.{AuthServiceConfig, CantonConfig, DbConfig}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.http.json.SprayJson
import com.digitalasset.canton.http.json.v2.{JsCommand, JsCommands}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.HttpServiceTestFixtureData
import com.digitalasset.canton.integration.tests.jsonapi.{
  HttpServiceTestFixture,
  HttpServiceUserFixture,
  HttpTestFuns,
}
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthServiceJWTSuppressionRule
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.integration.tests.ledgerapi.services.TestCommands
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentSetupPlugin,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import io.circe.syntax.EncoderOps
import monocle.macros.syntax.lens.*
import org.apache.pekko.http.scaladsl.model.{StatusCode, StatusCodes, Uri}

import java.io.File
import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class TokenExpirationVerificationIT
    extends CantonFixture
    with SecurityTags
    with TestCommands
    with HttpTestFuns
    with HttpServiceUserFixture.UserToken
    with ErrorsAssertions {

  registerPlugin(ExpectedScopeOverrideConfig(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override protected def adminToken: StandardJWTPayload = standardToken(
    participantAdmin,
    expiresIn = Some(Duration.ofNanos(FiniteDuration(2, scala.concurrent.duration.MINUTES).toNanos)),
  )
  override protected def packageFiles: List[File] =
    List(super.darFile)

  import com.digitalasset.canton.http.json.v2.JsCommandServiceCodecs.*

  def serviceCallName: String =
    "JSON API"

  private def expect(code: StatusCode)(call: => Future[StatusCode]) =
    for {
      result <- call
    } yield result should be(code)

  private def submitAndWait(
      fixture: HttpServiceTestFixtureData,
      token: Option[String],
      userId: String,
      party: String,
  ): Future[StatusCode] = {
    val headers = HttpServiceTestFixture.authorizationHeader(Jwt(token.getOrElse("")))
    val command1 = JsCommand.CreateCommand(
      templateId = templateIds.dummy,
      createArguments = io.circe.parser.parse(s"""{"operator":"$party"}""").value,
    )
    val jsReq = JsCommands(
      commands = Seq(command1),
      workflowId = None,
      commandId = s"$serviceCallName-${UUID.randomUUID}",
      actAs = Seq(party),
      userId = Some(userId),
      minLedgerTimeAbs = None,
      minLedgerTimeRel = None,
      readAs = Seq(),
      submissionId = None,
      disclosedContracts = Seq.empty,
      synchronizerId = None,
      packageIdSelectionPreference = Seq.empty,
      deduplicationPeriod = Some(com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod.Empty),
    )
    for {
      result <- postJsonRequest(
        uri = fixture.uri.withPath(Uri.Path("/v2/commands/submit-and-wait")),
        json = SprayJson
          .parse(jsReq.asJson.noSpaces)
          .valueOr(err => fail(s"$err")),
        headers = headers,
      )
    } yield (result._1)
  }

  def partyCall(
      fixture: HttpServiceTestFixtureData,
      context: ServiceCallContext,
  )(implicit env: TestConsoleEnvironment): Future[StatusCode] =
    submitAndWait(
      fixture = fixture,
      token = context.token,
      userId = context.userId,
      party = getPartyId(randomParty),
    )

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

  override val defaultScope: String = ExpectedScope

  private val nolifetimeCheckScope = "nolifetimeCheckScope"

  private val maxlifeZeroScope = "maxlifeZeroScope"

  val scopeBaseToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = participantAdmin,
    exp = None,
    format = StandardJWTTokenFormat.Scope,
    audiences = List.empty,
    scope = Some(defaultScope),
  )

  val noLifetimeCheckScopeToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = participantAdmin,
    exp = None,
    format = StandardJWTTokenFormat.Scope,
    audiences = List.empty,
    scope = Some(nolifetimeCheckScope),
  )

  val maxlife0ScopeToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = participantAdmin,
    exp = None,
    format = StandardJWTTokenFormat.Scope,
    audiences = List.empty,
    scope = Some(maxlifeZeroScope),
  )

  val userTokenWithoutExpiration = scopeBaseToken.copy(userId = randomPartyActUser)
  val userTokenWithShortExpiration =
    userTokenWithoutExpiration.copy(exp = Some(Instant.now().plus(2, ChronoUnit.MINUTES)))
  val userTokenWithLongExpiration =
    userTokenWithoutExpiration.copy(exp = Some(Instant.now().plus(200, ChronoUnit.MINUTES)))

  val userTokenWithNolifetimeCheckScope = noLifetimeCheckScopeToken.copy(
    userId = randomPartyActUser
  )

  val userTokenWithMax0LifeScope = maxlife0ScopeToken.copy(
    userId = randomPartyActUser
  )

  "allow access when token has short expiration" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call requiring party rights with a user token with short expiration"
    ) in httpTestFixture { fixture =>
    implicit val env = provideEnvironment
    loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
      for {
        _ <- expect(StatusCodes.OK)(
          partyCall(fixture, toScopeContext(userTokenWithShortExpiration))
        )
      } yield ()
    }
  }

  "allow access to alternative Scope with no expiration restrictions" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call requiring party rights with a user token with no expiration set when there are no restrictions configured"
    ) in httpTestFixture { fixture =>
    implicit val env = provideEnvironment
    loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
      for {
        _ <- expect(StatusCodes.OK)(
          partyCall(fixture, toScopeContext(userTokenWithNolifetimeCheckScope))
        )
      } yield ()
    }
  }

  "deny access when token has no expiration" taggedAs securityAsset
    .setAttack(
      Attack("User", "Uses stolen token with no expiration", "Block tokens with no expiration")
    ) in httpTestFixture { fixture =>
    implicit val env = provideEnvironment
    loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
      for {
        _ <- expect(StatusCodes.Unauthorized)(
          partyCall(fixture, toScopeContext(userTokenWithoutExpiration))
        )
      } yield ()
    }
  }

  "deny access when token max life is 0" in httpTestFixture { fixture =>
    implicit val env = provideEnvironment
    loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
      for {
        _ <- expect(StatusCodes.Unauthorized)(
          partyCall(fixture, toScopeContext(userTokenWithMax0LifeScope))
        )
      } yield ()
    }
  }

  "deny access when token has long expiration" taggedAs securityAsset
    .setAttack(
      Attack(
        "User",
        "Uses stolen token with very long expiration",
        "Block tokens with long expiration",
      )
    ) in httpTestFixture { fixture =>
    implicit val env = provideEnvironment
    loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
      for {
        _ <- expect(StatusCodes.Unauthorized)(
          partyCall(fixture, toScopeContext(userTokenWithLongExpiration))
        )
      } yield ()
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
                  targetScope = Some(ExpectedScope),
                  maxTokenLife = canton.config.NonNegativeDuration(
                    FiniteDuration(2, scala.concurrent.duration.MINUTES)
                  ),
                ),
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = jwtSecret,
                  targetAudience = None,
                  targetScope = Some(maxlifeZeroScope),
                  maxTokenLife = canton.config.NonNegativeDuration(
                    FiniteDuration(0, scala.concurrent.duration.MINUTES)
                  ),
                ),
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = jwtSecret,
                  targetAudience = None,
                  targetScope = Some(nolifetimeCheckScope),
                ),
            )
          )
          .focus(_.adminApi.adminTokenConfig.fixedAdminToken)
          .replace(Some(fallbackToken.secret))
      }(config)
  }
}
