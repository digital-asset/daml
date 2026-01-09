// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.jwt.{Jwt, StandardJWTPayload, StandardJWTTokenFormat}
import com.daml.ledger.api.v2.admin.user_management_service
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.http.json.v2.JsUserManagementCodecs.*
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.auth.ServiceCallContext
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.integration.tests.ledgerapi.services.TestCommands
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory, SuppressionRule}
import io.circe.syntax.EncoderOps
import monocle.macros.syntax.lens.*
import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}

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
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

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

  lazy val adminHeaders: List[org.apache.pekko.http.scaladsl.model.HttpHeader] =
    HttpServiceTestFixture.authorizationHeader(
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
              user_management_service
                .CreateUserRequest(
                  user = Some(
                    user_management_service.User(
                      id = randomUser,
                      primaryParty = "",
                      isDeactivated = false,
                      metadata = None,
                      identityProviderId = "",
                    )
                  ),
                  rights = Nil,
                )
                .asJson,
              adminHeaders,
            )
            .map { case (status, _) =>
              status should be(StatusCodes.OK)
            }
        } yield result,
        { logSeq =>
          val expectedLogsRegex = Seq(
            "Request POST /v2/users by http.* received a message",
            "Request POST /v2/users by http.* auth claims",
            "Request .*CreateUser by in-process-grpc.* auth claims",
            "Request .*CreateUser by in-process-grpc.* received a message",
            "Request .*CreateUser by in-process-grpc.* sending response",
            "Request .*CreateUser by in-process-grpc.* succeeded\\(OK\\)",
            "Request .*CreateUser by in-process-grpc.* completed",
            "Request POST /v2/users by http.* 200 Ok",
          )

          // Assert all expected logs are logged in order
          val (_, relevantLogs) = expectedLogsRegex.foldLeft(logSeq -> Seq.empty[LogEntry]) {
            case ((remainingSeq, relevantLogsAcc), nextExpectedLogRegex) =>
              val remainingWithFirstEntryMatchingRegex =
                remainingSeq.dropWhile(!_.message.matches(s".*$nextExpectedLogRegex.*"))
              val logMatchingRegex = remainingWithFirstEntryMatchingRegex.headOption.toList

              if (logMatchingRegex.isEmpty)
                fail(
                  s"Expected to find log matching regex in the remaining log sequence: $nextExpectedLogRegex but none found in $remainingSeq"
                )

              val newRemainingSeq = remainingWithFirstEntryMatchingRegex.tail
              val newRelevantLogs = relevantLogsAcc ++ logMatchingRegex

              newRemainingSeq -> newRelevantLogs
          }

          // all logs should have the same "trace-id"
          relevantLogs.map(_.mdc.get("trace-id").value).toSet.size should be(1)
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
