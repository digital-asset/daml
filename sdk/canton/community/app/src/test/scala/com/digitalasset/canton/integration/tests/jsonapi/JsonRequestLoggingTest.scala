// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.jwt.{Jwt, StandardJWTPayload, StandardJWTTokenFormat}
import com.daml.ledger.api.v2.admin.user_management_service
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.config.{CantonConfig, DbConfig}
import com.digitalasset.canton.http.json.v2.JsUserManagementCodecs.*
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.auth.ServiceCallContext
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.integration.tests.ledgerapi.services.TestCommands
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressionRule}
import io.circe.syntax.EncoderOps
import monocle.macros.syntax.lens.*
import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}
import spray.json.JsonParser

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

class JsonRequestLoggingTest
    extends CantonFixture
    with TestCommands
    with HttpTestFuns
    with HttpServiceUserFixture.UserToken
    with ErrorsAssertions {

  registerPlugin(ExpectedScopeOverrideConfig(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

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
    "log access to JSON and gRPC Ledger API" in httpTestFixture { fixture =>
      val randomUser: String = "randomUser-" + UUID.randomUUID.toString
      loggerFactory.assertLogsSeq(SuppressionRule.forLogger[ApiRequestLogger])(
        for {
          result <- fixture
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
        } yield result,
        { logSeq =>
          // all logs should have the same "trace-id"
          val traceIds = logSeq.map(_.mdc.get("trace-id")).toSet.size
          if (traceIds > 1) {
            fail(
              s"Expected all log entries to have the same trace-id, found: $logSeq"
            )
          }
          logSeq.map(_.mdc.get("trace-id")).toSet.size should be(1)
          // total 8 log messages
          logSeq.size should be(8)
          // 3 messages are from http
          logSeq.count(_.message.contains("http:")) should be(3)
          // the rest from grpc
          logSeq.count(_.message.contains("in-process-grpc:")) should be(5)
          // one http and one grpc message for the request
          logSeq.count(_.message.contains("received a message")) should be(2)
          // one http and one grpc message for the claims
          logSeq.count(_.message.contains("Claims")) should be(2)
          // one grpc success
          logSeq.count(_.message.contains("succeeded(OK)")) should be(1)
          // one http success
          logSeq.count(_.message.contains("200 Ok")) should be(1)
        },
      )
    }

  }

  case class ExpectedScopeOverrideConfig(
      protected val loggerFactory: NamedLoggerFactory
  ) extends EnvironmentSetupPlugin {
    override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
      config
        .focus(_.monitoring.logging.api)
        .modify(_.copy(messagePayloads = true, debugInProcessRequests = true))
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
