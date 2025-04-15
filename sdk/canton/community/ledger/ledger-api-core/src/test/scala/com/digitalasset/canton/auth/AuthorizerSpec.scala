// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.{BaseTest, LfLedgerString}
import com.digitalasset.daml.lf.data.Ref
import io.grpc.{Status, StatusRuntimeException}
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import scalapb.lenses.Lens

import java.time.Instant
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class AuthorizerSpec
    extends AsyncFlatSpec
    with BaseTest
    with Matchers
    with MockitoSugar
    with PekkoBeforeAndAfterAll {

  private val className = classOf[Authorizer].getSimpleName
  private val dummyRequest = 1337L
  private val expectedSuccessfulResponse = "expectedSuccessfulResponse"
  private val dummyReqRes: Long => Future[String] =
    Map(dummyRequest -> Future.successful(expectedSuccessfulResponse))

  behavior of s"$className.authorize"

  it should "authorize if claims are valid" in {
    contextWithClaims(ClaimSet.Claims.Wildcard) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.Admin())(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return permission denied authorization error for empty claims" in {
    contextWithClaims(ClaimSet.Claims.Empty) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.Admin())(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "return permission denied on authorization error for missing claims" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimPublic))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.Admin())(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "return permission denied " in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimPublic))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.Admin())(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  behavior of s"$className.authorize for RequiredClaim.Public"

  it should "return permission denied for for Admin only" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimAdmin))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.Public())(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "authorize for ClaimSet.Claims.Admin" in {
    contextWithClaims(ClaimSet.Claims.Admin) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.Public())(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "authorize for Public" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimPublic))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.Public())(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return permission denied for ClaimIdentityProviderAdmin" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimIdentityProviderAdmin))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.Public())(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  behavior of s"$className.authorize for RequiredClaim.Admin"

  it should "return permission denied for ClaimIdentityProviderAdmin" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimIdentityProviderAdmin))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.Admin())(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "authorize for for Admin only" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimAdmin))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.Admin())(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "authorize for ClaimSet.Claims.Admin" in {
    contextWithClaims(ClaimSet.Claims.Admin) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.Admin())(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  behavior of s"$className.authorize for RequiredClaim.AdminOrIdpAdmin"

  it should "authorize for ClaimIdentityProviderAdminOrIdpAdmin" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimIdentityProviderAdmin))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.AdminOrIdpAdmin())(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "authorize for for Admin onlyOrIdpAdmin" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimAdmin))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.AdminOrIdpAdmin())(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "authorize for ClaimSet.Claims.AdminOrIdpAdmin" in {
    contextWithClaims(ClaimSet.Claims.Admin) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.AdminOrIdpAdmin())(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  behavior of s"$className.authorize for RequiredClaim.ReadAs"

  it should "return permission denied for ClaimSet.Claims.Admin" in {
    contextWithClaims(ClaimSet.Claims.Admin) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAs("party"))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "return permission denied for Public" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimPublic))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAs("party"))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "authorize for ClaimSet.Claims.ReadAsAnyParty" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsAnyParty))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAs("party"))(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "authorize for ClaimSet.Claims.ActAsAnyParty" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsAnyParty))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAs("party"))(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "authorize for ClaimSet.Claims.ReadAs with same party" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Seq(ClaimReadAsParty(Ref.Party.assertFromString("party")))
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAs("party"))(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "authorize for ClaimSet.Claims.ActAs with same party" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(Ref.Party.assertFromString("party"))))
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAs("party"))(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return permission denied for ClaimSet.Claims.ReadAs with different party" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Seq(ClaimReadAsParty(Ref.Party.assertFromString("party2")))
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAs("party"))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "return permission denied for ClaimSet.Claims.ActAs with different party" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Seq(ClaimActAsParty(Ref.Party.assertFromString("party2")))
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAs("party"))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  behavior of s"$className.authorize for RequiredClaim.ReadAsAnyParty"

  it should "return permission denied for ClaimSet.Claims.AdminAnyParty" in {
    contextWithClaims(ClaimSet.Claims.Admin) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAsAnyParty())(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "return permission denied for PublicAnyParty" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimPublic))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAsAnyParty())(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "authorize for ClaimSet.Claims.ReadAsAnyPartyAnyParty" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsAnyParty))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAsAnyParty())(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "authorize for ClaimSet.Claims.ActAsAnyPartyAnyParty" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsAnyParty))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAsAnyParty())(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return permission denied for ClaimSet.Claims.ReadAs a partyAnyParty" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Seq(ClaimReadAsParty(Ref.Party.assertFromString("party")))
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAsAnyParty())(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "return permission denied for ClaimSet.Claims.ActAs with a partyAnyParty" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(Ref.Party.assertFromString("party"))))
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ReadAsAnyParty())(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  behavior of s"$className.authorize for RequiredClaim.ActAs"

  it should "return permission denied for ClaimSet.Claims.Admin" in {
    contextWithClaims(ClaimSet.Claims.Admin) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ActAs("party"))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "return permission denied for Public" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimPublic))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ActAs("party"))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "return permission denied for ClaimSet.Claims.ReadAsAnyParty" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsAnyParty))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ActAs("party"))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "authorize for ClaimSet.Claims.ActAsAnyParty" in {
    contextWithClaims(ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsAnyParty))) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ActAs("party"))(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return permission denied for ClaimSet.Claims.ReadAs with same party" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Seq(ClaimReadAsParty(Ref.Party.assertFromString("party")))
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ActAs("party"))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "authorize for ClaimSet.Claims.ActAs with same party" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(Ref.Party.assertFromString("party"))))
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ActAs("party"))(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return permission denied for ClaimSet.Claims.ActAs with different party" in {
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Seq(ClaimActAsParty(Ref.Party.assertFromString("party2")))
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.ActAs("party"))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  behavior of s"$className.authorize for RequiredClaim.MatchUserId"

  it should "authorize for resolvedFromUser and user ID matching user ID" in {
    val userL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        userId = Some(dummyRequest.toString),
        resolvedFromUser = true,
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserIdForUserManagement(userL))(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return permission denied for resolvedFromUser and user ID not matching user ID" in {
    val userL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        userId = Some("x"),
        resolvedFromUser = true,
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserIdForUserManagement(userL))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "return permission denied for not resolvedFromUser" in {
    val userL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        resolvedFromUser = false,
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserIdForUserManagement(userL))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "return permission denied for resolvedFromUser and not defined user ID" in {
    val userL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        resolvedFromUser = false,
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserIdForUserManagement(userL))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "authorize for resolvedFromUser and user ID not matching user ID if Admin rights available" in {
    val userL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Seq(ClaimAdmin),
        userId = Some("x"),
        resolvedFromUser = true,
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserIdForUserManagement(userL))(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "authorize for resolvedFromUser and user ID not matching user ID if IDP Admin rights available" in {
    val userL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Seq(ClaimIdentityProviderAdmin),
        userId = Some("x"),
        resolvedFromUser = true,
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserIdForUserManagement(userL))(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  behavior of s"$className.authorize for RequiredClaim.MatchUserId"

  it should "authorize for user ID matching user ID" in {
    val userIdL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        userId = Some(dummyRequest.toString),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserId(userIdL))(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "authorize for no user ID specified but available in the claims" in {
    val userIdL = Lens[Long, String](l => if (l == 0) "" else l.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        userId = Some(dummyRequest.toString),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserId(userIdL))(0)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return permission denied for user ID not matching user ID" in {
    val userIdL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        userId = Some("15"),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserId(userIdL))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "return permission denied for user ID not matching user ID if skipUserIdValidationForAnyPartyReaders" in {
    val userIdL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        userId = Some("15"),
      )
    ) {
      authorizer().rpc(dummyReqRes)(
        RequiredClaim.MatchUserId(
          userIdL,
          skipUserIdValidationForAnyPartyReaders = true,
        )
      )(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "authorize for user ID not matching user ID for AnyPartyReaders-s if skipUserIdValidationForAnyPartyReaders" in {
    val userIdL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Seq(ClaimReadAsAnyParty),
        userId = Some("15"),
      )
    ) {
      authorizer().rpc(dummyReqRes)(
        RequiredClaim.MatchUserId(
          userIdL,
          skipUserIdValidationForAnyPartyReaders = true,
        )
      )(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return invalid argument for no user ID in claims, and none provided in the request" in {
    val userIdL = Lens[Long, String](l => if (l == 0) "" else l.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        userId = None,
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchUserId(userIdL))(0)
    }
      .transform(
        assertExpectedFailure(Status.INVALID_ARGUMENT.getCode)
      )
  }

  behavior of s"$className.authorize for RequiredClaim.MatchIdentityProviderId"

  it should "authorize for identity provider ID in claims matching provided identity provider ID" in {
    val idpIdL = Lens[Long, String](_.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        identityProviderId = Some(LfLedgerString.assertFromString(dummyRequest.toString)),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchIdentityProviderId(idpIdL))(dummyRequest)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return invalid argument for malformed provided identity provider ID" in {
    val idpIdL = Lens[Long, String](_ => "!@#$%^%&^^&*&*)")((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        identityProviderId = Some(LfLedgerString.assertFromString(dummyRequest.toString)),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchIdentityProviderId(idpIdL))(dummyRequest)
    }
      .transform(
        assertExpectedFailure(Status.INVALID_ARGUMENT.getCode)
      )
  }

  it should "authorize for identity provider ID in claims with no identity provider ID provided in the request" in {
    val idpIdL = Lens[Long, String](l => if (l == 0) "" else l.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        identityProviderId = Some(LfLedgerString.assertFromString(dummyRequest.toString)),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchIdentityProviderId(idpIdL))(0)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  it should "return permission denied for identity provider ID in claims with no identity provider ID provided in the request for Admins (for admins no auto-resolution from claims)" in {
    val idpIdL = Lens[Long, String](l => if (l == 0) "" else l.toString)((_, s) => s.toLong)
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Seq(ClaimAdmin),
        identityProviderId = Some(LfLedgerString.assertFromString(dummyRequest.toString)),
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchIdentityProviderId(idpIdL))(0)
    }
      .transform(
        assertExpectedFailure(Status.PERMISSION_DENIED.getCode)
      )
  }

  it should "authorize for no identity provider ID in claims with no identity provider ID provided in the request (resolving to the default provider)" in {
    val idpIdL = Lens[Long, String](l => if (l == 0) "" else l.toString)((_, s) =>
      if (s == "") dummyRequest else s.toLong
    )
    contextWithClaims(
      ClaimSet.Claims.Empty.copy(
        claims = Nil,
        identityProviderId = None,
      )
    ) {
      authorizer().rpc(dummyReqRes)(RequiredClaim.MatchIdentityProviderId(idpIdL))(0)
    }.map(_ shouldBe expectedSuccessfulResponse)
  }

  private def assertExpectedFailure[T](
      expectedStatusCode: Status.Code
  ): Try[T] => Try[Assertion] = {
    case Failure(ex: StatusRuntimeException) =>
      ex.getStatus.getCode shouldBe expectedStatusCode
      Success(succeed)
    case ex => fail(s"Expected a failure with StatusRuntimeException but got $ex")
  }

  private def contextWithClaims[R](claims: ClaimSet.Claims)(f: => R): R =
    io.grpc.Context.ROOT
      .withValue(AuthInterceptor.contextKeyClaimSet, claims)
      .call(() => f)

  private def authorizer() = new Authorizer(
    () => Instant.ofEpochSecond(1337L),
    "participant-id",
    telemetry = NoOpTelemetry,
    loggerFactory = loggerFactory,
  )
}
