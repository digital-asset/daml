// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.jwt.{Jwt, StandardJWTPayload, StandardJWTTokenFormat}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.auth.{AccessLevel, CantonAdminToken}
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.{AuthServiceConfig, CantonConfig}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.http.json.Circe
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.http.json.v2.{JsCommand, JsCommands}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
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
import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.Future

class JsonApiAuthIT
    extends CantonFixture
    with SecurityTags
    with TestCommands
    with HttpTestFuns
    with HttpServiceUserFixture.UserToken
    with ErrorsAssertions {

  registerPlugin(ExpectedScopeOverrideConfig(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override protected def packageFiles: List[File] =
    List(super.darFile)

  import com.digitalasset.canton.http.json.v2.JsCommandServiceCodecs.*

  def serviceCallName: String =
    "JSON API"

  private def expect(code: StatusCode)(call: => Future[StatusCode]) =
    for {
      result <- call
    } yield result should be(code)

  private def serviceCall(
      fixture: HttpServiceTestFixtureData,
      context: ServiceCallContext,
  ): Future[StatusCode] = serviceCallWithResult(
    fixture = fixture,
    context = context,
  ).map { case (code, _) =>
    code
  }

  private def serviceCallWithResult(
      fixture: HttpServiceTestFixtureData,
      context: ServiceCallContext,
  ): Future[(StatusCode, String)] = {

    val headers = HttpServiceTestFixture.authorizationHeader(Jwt(context.token.getOrElse("")))
    fixture.getRequestString(Uri.Path("/v2/parties"), headers)

  }

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
        json = Circe.parse(jsReq.asJson.noSpaces).fold(err => fail(s"$err"), identity),
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

  private def toAudienceContext(
      payload: StandardJWTPayload
  ): ServiceCallContext = ServiceCallContext(
    token = Some(
      toHeader(
        payload = payload,
        enforceFormat = Some(StandardJWTTokenFormat.Audience),
        secret = audienceJwtSecret.unwrap,
      )
    )
  )

  private def toPrivilegedScpContext(
      payload: StandardJWTPayload
  ): ServiceCallContext = ServiceCallContext(
    token = Some(
      toHeader(
        payload = payload,
        enforceFormat = Some(StandardJWTTokenFormat.Scope),
        secret = privilegedScpJwtSecret.unwrap,
      )
    )
  )

  private def toPrivilegedAudContext(
      payload: StandardJWTPayload
  ): ServiceCallContext = ServiceCallContext(
    token = Some(
      toHeader(
        payload = payload,
        enforceFormat = Some(StandardJWTTokenFormat.Audience),
        secret = privilegedAudJwtSecret.unwrap,
      )
    )
  )

  private val fallbackToken = CantonAdminToken.create(new SymbolicPureCrypto())

  private val privilegedScpJwtSecret = NonEmptyString.tryCreate(UUID.randomUUID().toString)
  private val privilegedAudJwtSecret = NonEmptyString.tryCreate(UUID.randomUUID().toString)
  private val audienceJwtSecret = NonEmptyString.tryCreate(UUID.randomUUID().toString)

  override val defaultScope: String = ExpectedScope
  private val privilegedScope: String = "Privileged-Scope"
  private val privilegedAudience: String = "Privileged-Audience"
  // This user does not exist in the user management
  val privilegedEphemeralUser = "PrivilegedUser"

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
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Scope,
    audiences = List.empty,
    scope = Some(defaultScope),
  )
  val scope: Tokens = Tokens(
    user = scopeBaseToken.copy(userId = randomPartyActUser),
    admin = scopeBaseToken,
    additionalDiscriminator = scopeBaseToken.copy(scope = Some(s"$defaultScope additionalScope")),
    noDiscriminator = scopeBaseToken.copy(scope = None),
    wrongDiscriminator = scopeBaseToken.copy(scope = Some("scope1 scope2")),
    mixedDiscriminators = scopeBaseToken
      .copy(audiences = List(ExpectedAudience), scope = Some(s"$privilegedScope $ExpectedScope")),
    expired = scopeBaseToken.copy(exp = Some(Instant.now().plusNanos(Duration.ofDays(-1).toNanos))),
    unknownUser = scopeBaseToken.copy(userId = "unknown_user"),
  )

  val audienceBaseToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = participantAdmin,
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Audience,
    audiences = List(ExpectedAudience),
    scope = None,
  )
  val audience: Tokens = Tokens(
    user = audienceBaseToken.copy(userId = randomPartyActUser),
    admin = audienceBaseToken,
    additionalDiscriminator =
      audienceBaseToken.copy(audiences = List(ExpectedAudience, "additionalAud")),
    noDiscriminator = audienceBaseToken.copy(audiences = Nil),
    wrongDiscriminator = audienceBaseToken.copy(audiences = List("aud1", "aud2")),
    mixedDiscriminators = audienceBaseToken
      .copy(audiences = List(ExpectedAudience), scope = Some(s"$privilegedScope $ExpectedScope")),
    expired =
      audienceBaseToken.copy(exp = Some(Instant.now().plusNanos(Duration.ofDays(-1).toNanos))),
    unknownUser = audienceBaseToken.copy(userId = "unknown_user"),
  )

  val privilegedScpBaseToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = privilegedEphemeralUser,
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Scope,
    audiences = List.empty,
    scope = Some(privilegedScope),
  )
  val privilegedScp: Tokens = Tokens(
    user = privilegedScpBaseToken,
    admin =
      privilegedScpBaseToken, // there is no distinction between party and admin users for privilegedScp tokens
    additionalDiscriminator =
      privilegedScpBaseToken.copy(scope = Some(s"$privilegedScope additionalScope")),
    noDiscriminator = privilegedScpBaseToken.copy(scope = None),
    wrongDiscriminator = privilegedScpBaseToken.copy(scope = Some("scope1 scope2")),
    mixedDiscriminators = privilegedScpBaseToken
      .copy(audiences = List(ExpectedAudience), scope = Some(s"$ExpectedScope $privilegedScope")),
    expired =
      privilegedScpBaseToken.copy(exp = Some(Instant.now().plusNanos(Duration.ofDays(-1).toNanos))),
    unknownUser = privilegedScpBaseToken.copy(userId = "unknown_user"),
  )

  val privilegedAudBaseToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = privilegedEphemeralUser,
    exp = Some(Instant.now().plusNanos(Duration.ofMinutes(5).toNanos)),
    format = StandardJWTTokenFormat.Audience,
    audiences = List(privilegedAudience),
    scope = None,
  )
  val privilegedAud: Tokens = Tokens(
    user = privilegedAudBaseToken,
    admin =
      privilegedAudBaseToken, // there is no distinction between party and admin users for privilegedAud tokens
    additionalDiscriminator =
      privilegedAudBaseToken.copy(audiences = List(privilegedAudience, "audience1")),
    noDiscriminator = privilegedAudBaseToken.copy(audiences = List.empty),
    wrongDiscriminator = privilegedAudBaseToken.copy(audiences = List("audience1", "audience2")),
    mixedDiscriminators = privilegedAudBaseToken
      .copy(audiences = List(ExpectedAudience, privilegedAudience), scope = Some(ExpectedScope)),
    expired =
      privilegedAudBaseToken.copy(exp = Some(Instant.now().plusNanos(Duration.ofDays(-1).toNanos))),
    unknownUser = privilegedAudBaseToken.copy(userId = "unknown_user"),
  )

  serviceCallName should {
    "allow access to an endpoint requiring admin rights with fall-back authorization" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a canton admin token"
      ) in httpTestFixture { fixture =>
      expect(StatusCodes.OK) {
        serviceCall(fixture, ServiceCallContext(token = Some(fallbackToken.secret)))
      }
    }

    "allow access to an endpoint requiring admin rights with admin token" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call requiring admin rights with an admin token"
      ) in httpTestFixture { fixture =>
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        for {
          _ <- expect(StatusCodes.OK)(
            serviceCall(fixture, toScopeContext(scope.admin))
          )
          _ <- expect(StatusCodes.OK)(
            serviceCall(fixture, toAudienceContext(audience.admin))
          )
          _ <- expect(StatusCodes.OK)(
            serviceCall(fixture, toPrivilegedScpContext(privilegedScp.admin))
          )
          _ <- expect(StatusCodes.OK)(
            serviceCall(fixture, toPrivilegedAudContext(privilegedAud.admin))
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
      implicit val env = provideEnvironment
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        for {
          _ <- expect(StatusCodes.Forbidden)(
            partyCall(fixture, toScopeContext(scope.admin))
          )
          _ <- expect(StatusCodes.Forbidden)(
            partyCall(fixture, toAudienceContext(audience.admin))
          )
          _ <- expect(StatusCodes.Forbidden)(
            partyCall(fixture, toPrivilegedScpContext(privilegedScp.admin))
          )
          _ <- expect(StatusCodes.Forbidden)(
            partyCall(fixture, toPrivilegedAudContext(privilegedAud.admin))
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
          _ <- expect(StatusCodes.Forbidden)(
            serviceCall(fixture, toScopeContext(scope.user))
          )
          _ <- expect(StatusCodes.Forbidden)(
            serviceCall(fixture, toAudienceContext(audience.user))
          )
        } yield ()
      }
    // There is no concept of user token for privilegedScp and privilegedAud jwt
    }

    "allow access to an endpoint requiring party rights with user token" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call requiring party rights with a user token"
      ) in httpTestFixture { fixture =>
      implicit val env = provideEnvironment
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        for {
          _ <- expect(StatusCodes.OK)(
            partyCall(fixture, toScopeContext(scope.user))
          )
          _ <- expect(StatusCodes.OK)(
            partyCall(fixture, toAudienceContext(audience.user))
          )
        } yield ()
        // There is no concept of user token for privilegedScp and privilegedAud jwt
      }
    }

    "allow access to an endpoint with a token with additional audiences/scopes" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a token containing additional audiences/scopes"
      ) in httpTestFixture { fixture =>
//      import env
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        for {
          _ <- expect(StatusCodes.OK)(
            serviceCall(fixture, toScopeContext(scope.additionalDiscriminator))
          )
          _ <- expect(StatusCodes.OK)(
            serviceCall(fixture, toAudienceContext(audience.additionalDiscriminator))
          )
          _ <- expect(StatusCodes.OK)(
            serviceCall(fixture, toPrivilegedScpContext(privilegedScp.additionalDiscriminator))
          )
          _ <- expect(StatusCodes.OK)(
            serviceCall(fixture, toPrivilegedAudContext(privilegedAud.additionalDiscriminator))
          )
        } yield ()
      }
    }

    "deny access to an endpoint with a token without audience/scope" taggedAs securityAsset
      .setAttack(
        attackUnauthenticated(threat =
          "Ledger API client cannot make a call with a token with no intended audience/scope"
        )
      ) in httpTestFixture { fixture =>
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        for {
          _ <- expect(StatusCodes.Unauthorized)(
            serviceCall(fixture, toScopeContext(scope.noDiscriminator))
          )
          _ <- expect(StatusCodes.Unauthorized)(
            serviceCall(fixture, toAudienceContext(audience.noDiscriminator))
          )
          _ <- expect(StatusCodes.Unauthorized)(
            serviceCall(fixture, toPrivilegedScpContext(privilegedScp.noDiscriminator))
          )
          _ <- expect(StatusCodes.Unauthorized)(
            serviceCall(fixture, toPrivilegedAudContext(privilegedAud.noDiscriminator))
          )
        } yield ()
      }
    }

    "deny access to an endpoint with a token with wrong intended audience/scope" taggedAs securityAsset
      .setAttack(
        attackUnauthenticated(threat =
          "Ledger API client cannot make a call with a token with wrong intended audience/scope"
        )
      ) in httpTestFixture { fixture =>
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        for {
          _ <- expect(StatusCodes.Unauthorized)(
            serviceCall(fixture, toScopeContext(scope.wrongDiscriminator))
          )
          _ <- expect(StatusCodes.Unauthorized)(
            serviceCall(fixture, toAudienceContext(audience.wrongDiscriminator))
          )
          _ <- expect(StatusCodes.Unauthorized)(
            serviceCall(fixture, toPrivilegedScpContext(privilegedScp.wrongDiscriminator))
          )
          _ <- expect(StatusCodes.Unauthorized)(
            serviceCall(fixture, toPrivilegedAudContext(privilegedAud.wrongDiscriminator))
          )
        } yield ()
      }
    }

    "allow access to an endpoint with a token that mixes expected audiences/scopes" taggedAs securityAsset
      .setHappyCase(
        "Ledger API client can make a call with a token with multiple expected audiences/scopes"
      ) in httpTestFixture { fixture =>
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
        for {
          _ <- expect(StatusCodes.OK)(
            serviceCall(fixture, toScopeContext(scope.mixedDiscriminators))
          )
          _ <- expect(StatusCodes.OK)(
            serviceCall(fixture, toAudienceContext(audience.mixedDiscriminators))
          )
          _ <- expect(StatusCodes.OK)(
            serviceCall(fixture, toPrivilegedScpContext(privilegedScp.mixedDiscriminators))
          )
          _ <- expect(StatusCodes.OK)(
            serviceCall(fixture, toPrivilegedAudContext(privilegedAud.mixedDiscriminators))
          )
        } yield ()
      }
    }

    "deny access to an endpoint with an expired token" taggedAs adminSecurityAsset.setAttack(
      attackUnauthenticated(threat = "Present an expired token")
    ) in httpTestFixture { fixture =>
      loggerFactory.suppress(AuthServiceJWTSuppressionRule) {

        for {
          _ <- expect(StatusCodes.Unauthorized)(serviceCall(fixture, toScopeContext(scope.expired)))
          _ <- expect(StatusCodes.Unauthorized)(
            serviceCall(fixture, toAudienceContext(audience.expired))
          )
          _ <- expect(StatusCodes.Unauthorized)(
            serviceCall(fixture, toPrivilegedScpContext(privilegedScp.expired))
          )
          _ <- expect(StatusCodes.Unauthorized)(
            serviceCall(fixture, toPrivilegedAudContext(privilegedAud.expired))
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
          _ <- expect(StatusCodes.Forbidden)(
            serviceCallWithResult(fixture, toScopeContext(scope.unknownUser)).map {
              case (code, result) =>
                io.circe.parser
                  .decode[JsCantonError](result)
                  .value
                  .context(JsCantonError.ledgerApiErrorContext) should be(
                  JsCantonError.tokenProblemError._2
                )
                code
            }
          )
          _ <- expect(StatusCodes.Forbidden)(
            serviceCall(fixture, toAudienceContext(audience.unknownUser))
          )
        } yield ()
        // There is no concept of user token for privilegedScp and privilegedAud jwt
      }
    }

    "deny unauthenticated access" taggedAs securityAsset.setAttack(
      attackUnauthenticated(threat = "Do not present token")
    ) in httpTestFixture { fixture =>
      expect(StatusCodes.BadRequest)(
        serviceCall(fixture, noToken)
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
                  targetScope = Some(ExpectedScope),
                ),
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = audienceJwtSecret,
                  targetAudience = Some(ExpectedAudience),
                  targetScope = None,
                ),
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = privilegedScpJwtSecret,
                  targetAudience = None,
                  targetScope = Some(privilegedScope),
                  privileged = true,
                  accessLevel = AccessLevel.Admin,
                ),
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = privilegedAudJwtSecret,
                  targetAudience = Some(privilegedAudience),
                  targetScope = None,
                  privileged = true,
                  accessLevel = AccessLevel.Admin,
                ),
            )
          )
          .focus(_.ledgerApi.adminTokenConfig.fixedAdminToken)
          .replace(Some(fallbackToken.secret))
          .focus(_.ledgerApi.adminTokenConfig.adminClaim)
          .replace(true)
      }(config)
  }
}
