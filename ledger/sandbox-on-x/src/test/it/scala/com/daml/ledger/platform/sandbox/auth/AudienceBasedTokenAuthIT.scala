// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.error.ErrorsAssertions
import com.daml.ledger.api.auth.{StandardJWTPayload, StandardJWTTokenFormat}
import com.daml.ledger.api.v1.admin.package_management_service._
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest.singleParticipant
import com.daml.platform.config.JwtAudience
import com.daml.platform.sandbox.auth.AudienceBasedTokenAuthIT.ExpectedAudience
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

import java.io.File
import java.time.{Duration, Instant}
import scala.concurrent.Future

class AudienceBasedTokenAuthIT extends ServiceCallAuthTests with ErrorsAssertions {
  override def config: Config = super.config.copy(
    participants = singleParticipant(
      jwtAudience = JwtAudience(enabled = true, audience = Some(ExpectedAudience))
    )
  )

  override def expectsAudienceBasedTokens: Boolean = true

  override def serviceCallName: String = ""

  override def packageFiles: List[File] = List.empty

  override protected def serviceCall(context: ServiceCallContext): Future[Any] =
    stub(PackageManagementServiceGrpc.stub(channel), context.token)
      .listKnownPackages(ListKnownPackagesRequest())

  private def toContext(payload: StandardJWTPayload): ServiceCallContext = ServiceCallContext(
    Some(
      toHeader(
        payload,
        audienceBasedToken = true,
      )
    )
  )

  val expectedAudienceToken: StandardJWTPayload = StandardJWTPayload(
    issuer = None,
    participantId = None,
    userId = "participant_admin",
    exp = None,
    format = StandardJWTTokenFormat.ParticipantId,
    audiences = List(ExpectedAudience),
  )

  val multipleAudienceWithExpectedToken: StandardJWTPayload =
    expectedAudienceToken.copy(audiences = List(ExpectedAudience, "additionalAud"))

  val noAudienceWithExpectedToken: StandardJWTPayload =
    expectedAudienceToken.copy(audiences = List("aud1", "aud2"))

  val expiredToken: StandardJWTPayload =
    expectedAudienceToken.copy(exp = Some(Instant.now().plusNanos(Duration.ofDays(-1).toNanos)))

  val unknownUserToken: StandardJWTPayload =
    expectedAudienceToken.copy(userId = "unknown_user")

  it should "allow access to an endpoint with the token which is matching intended audience" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with a JWT with intended audience"
    ) in {
    expectSuccess {
      serviceCall(toContext(expectedAudienceToken))
    }
  }

  it should "allow access to an endpoint with the token with multiple audiences which is matching expected audience" taggedAs securityAsset
    .setHappyCase(
      "Ledger API client can make a call with a JWT with intended audience"
    ) in {
    expectSuccess {
      serviceCall(toContext(multipleAudienceWithExpectedToken))
    }
  }

  it should "deny access with no intended audience" taggedAs securityAsset.setAttack(
    attackUnauthenticated(threat =
      "Ledger API client can make a call with a JWT with no intended audience"
    )
  ) in {
    expectPermissionDenied(serviceCall(toContext(noAudienceWithExpectedToken)))
  }

  it should "deny calls with user token for 'unknown_user' without expiration" taggedAs adminSecurityAsset
    .setAttack(
      attackPermissionDenied(threat = "Present a user JWT for 'unknown_user' without expiration")
    ) in {
    expectPermissionDenied(serviceCall(toContext(unknownUserToken)))
  }

  it should "deny calls with an expired admin token" taggedAs adminSecurityAsset.setAttack(
    attackUnauthenticated(threat = "Present an expired admin JWT")
  ) in {
    expectUnauthenticated(serviceCall(toContext(expiredToken)))
  }

  it should "deny unauthenticated access" taggedAs securityAsset.setAttack(
    attackUnauthenticated(threat = "Do not present JWT")
  ) in {
    expectUnauthenticated(serviceCall(noToken))
  }

  it should "return invalid argument for custom token" taggedAs securityAsset.setAttack(
    attackUnauthenticated(threat = "Present a custom JWT")
  ) in {
    expectUnauthenticated(serviceCall(canReadAsAdmin))
  }
}

object AudienceBasedTokenAuthIT {
  private val ExpectedAudience = "ExpectedTargetAudience"
}
