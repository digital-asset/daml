// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package integrationtest

import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.time.{Duration, Instant}

import scala.util.{Failure, Success}

final class CantonFixtureAdditionalConfigTest
    extends AsyncWordSpec
    with CantonFixture
    with Matchers {

  private val secret = "additional-participant-config-test-secret"

  // Enable authentication through the free-form hook rather than `authSecret`,
  // so that observing auth-enabled behaviour proves the injected block reached
  // the participant's config.
  override protected lazy val additionalParticipantConfig: Option[String] = Some(
    s"""ledger-api.auth-services = [{
       |  type = unsafe-jwt-hmac-256
       |  secret = "$secret"
       |}]""".stripMargin
  )

  "additionalParticipantConfig" should {
    "be spliced into the participant config" in {
      for {
        // The injected auth config rejects tokenless clients: LedgerClient
        // validates its token at construction, so the failure surfaces here.
        // Without the injected block this construction succeeds and the test
        // fails.
        failure <- defaultLedgerClient()
          .transform {
            case Failure(e: StatusRuntimeException) => Success(e)
            case Failure(e) => Failure(new Exception(s"unexpected failure: $e"))
            case Success(_) => Failure(new Exception("unexpected success without token"))
          }
        // ... and accepts calls authenticated with the injected secret. The
        // token must carry an expiry within canton's default ledger-api
        // max-token-lifetime of 5 minutes.
        token = CantonRunner.getToken(
          CantonRunner.adminUserId,
          authSecret = Some(secret),
          exp = Some(Instant.now().plusNanos(Duration.ofMinutes(3).toNanos)),
        )
        authClient <- defaultLedgerClient(token)
        _ <- authClient.partyManagementClient.allocateParty(hint = None, token = None)
      } yield failure.getStatus.getCode shouldBe Status.Code.UNAUTHENTICATED
    }
  }
}
