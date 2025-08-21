// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.jwt.{Jwt, StandardJWTPayload, StandardJWTTokenFormat}
import com.daml.ledger.api.v2.admin.{identity_provider_config_service, user_management_service}
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.config.{AuthServiceConfig, CantonConfig, DbConfig}
import com.digitalasset.canton.http.json.v2.JsIdentityProviderCodecs.*
import com.digitalasset.canton.http.json.v2.JsUserManagementCodecs.*
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.jsonapi.AbstractHttpServiceIntegrationTestFuns.HttpServiceTestFixtureData
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.ApiUserManagementServiceSuppressionRule
import com.digitalasset.canton.integration.tests.ledgerapi.auth.ServiceCallContext
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.integration.tests.ledgerapi.services.TestCommands
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentSetupPlugin}
import com.digitalasset.canton.logging.NamedLoggerFactory
import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import monocle.macros.syntax.lens.*
import org.apache.pekko.http.scaladsl.model.Uri.Query
import org.apache.pekko.http.scaladsl.model.{HttpHeader, StatusCodes, Uri}
import org.scalatest.Assertion
import spray.json.JsonParser

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.concurrent.Future

class JsonUserApiTest
    extends CantonFixture
    with TestCommands
    with HttpTestFuns
    with HttpServiceUserFixture.UserToken
    with ErrorsAssertions {

  registerPlugin(ExpectedScopeOverrideConfig(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override val defaultScope: String = ExpectedScope

  val scopeBaseToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = "",
    exp = Some(Instant.now().plus(2, ChronoUnit.MINUTES)),
    format = StandardJWTTokenFormat.Scope,
    audiences = List.empty,
    scope = Some(defaultScope),
  )

  lazy val adminHeaders = HttpServiceTestFixture.authorizationHeader(
    Jwt(toScopeContext(adminToken).token.getOrElse(""))
  )

  "Json api" should {
    "return current user using data from token" in httpTestFixture { fixture =>
      forRandomUser(fixture) { case (randomUser, userHeaders) =>
        fixture.getRequestString(Uri.Path("/v2/authenticated-user"), userHeaders).map {
          case (status, result) =>
            status should be(StatusCodes.OK)
            decode[user_management_service.GetUserResponse](
              result
            ).value.getUser.id should be(randomUser)
        }
      }
    }

    "fail reading current user for wrong idp" in httpTestFixture { fixture =>
      loggerFactory.suppress(ApiUserManagementServiceSuppressionRule) {
        forRandomUser(fixture) { case (_, userHeaders) =>
          getRequestEncoded(
            fixture.uri withPath Uri.Path("/v2/authenticated-user") withQuery Query(
              ("identity-provider-id", "wrong-id")
            ),
            userHeaders,
          ).map { case (status, _) =>
            status should be(StatusCodes.Forbidden)
          }
        }
      }
    }

    "get user from created idp" in httpTestFixture { fixture =>
      val randomUserInIdp: String = "randomUserIdp-" + UUID.randomUUID.toString
      for {
        _ <- fixture
          .postJsonRequest(
            Uri.Path("/v2/idps"),
            JsonParser(
              identity_provider_config_service
                .CreateIdentityProviderConfigRequest(
                  Some(
                    identity_provider_config_service.IdentityProviderConfig(
                      identityProviderId = "idp-1",
                      isDeactivated = false,
                      issuer = "user-idp-test",
                      jwksUrl = "https://localhost",
                      audience = "",
                    )
                  )
                )
                .asJson
                .toString()
            ),
            adminHeaders,
          )
          .map { case (status, result) =>
            status should be(StatusCodes.OK)
          }
        _ <- fixture
          .postJsonRequest(
            Uri.Path("/v2/users"),
            JsonParser(
              user_management_service
                .CreateUserRequest(
                  user =
                    Some(user_management_service.User(randomUserInIdp, "", false, None, "idp-1")),
                  rights = Nil,
                )
                .asJson
                .toString()
            ),
            adminHeaders,
          )
          .map { case (status, result) =>
            status should be(StatusCodes.OK)
          }
        result <- getRequestEncoded(
          fixture.uri withPath Uri.Path(s"/v2/users/$randomUserInIdp") withQuery Query(
            ("identity-provider-id", "idp-1")
          ),
          adminHeaders,
        ).map { case (status, _) =>
          status should be(StatusCodes.OK)
        }
      } yield result
    }
  }

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
                )
            )
          )
      }(config)
  }

  private def forRandomUser(
      fixture: HttpServiceTestFixtureData
  )(test: (String, List[HttpHeader]) => Future[Assertion]) = {

    val randomUser: String = "randomUser-" + UUID.randomUUID.toString
    val userToken = scopeBaseToken.copy(userId = randomUser)
    val userHeaders = HttpServiceTestFixture.authorizationHeader(
      Jwt(toScopeContext(userToken).token.getOrElse(""))
    )
    for {
      _ <- fixture
        .postJsonRequest(
          Uri.Path("/v2/users"),
          JsonParser(
            user_management_service
              .CreateUserRequest(
                user = Some(user_management_service.User(randomUser, "", false, None, "")),
                rights = Nil,
              )
              .asJson
              .toString()
          ),
          adminHeaders,
        )
        .map { case (status, _) =>
          status should be(StatusCodes.OK)
        }
      result <- test(randomUser, userHeaders)
    } yield result
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
}
