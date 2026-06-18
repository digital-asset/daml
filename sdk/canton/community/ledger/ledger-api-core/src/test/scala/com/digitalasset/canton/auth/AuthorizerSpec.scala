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
import org.scalatest.Assertions.succeed
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

  import AuthorizerSpec.*

  private val className = classOf[Authorizer].getSimpleName
  private val dummyRequest = 1337L
  private val expectedSuccessfulResponse = "expectedSuccessfulResponse"
  private val dummyReqRes: Long => Future[String] =
    Map(dummyRequest -> Future.successful(expectedSuccessfulResponse))

  private val party = Ref.Party.assertFromString("party")
  private val party2 = Ref.Party.assertFromString("party2")

  behavior of s"$className.authorize for RequiredClaim.Public"

  List(
    TestDefinition(RequiredClaim.Public(), ClaimSet.Claims.Empty, expectedPermissionDenied),
    TestDefinition(RequiredClaim.Public(), ClaimSet.Claims.Wildcard, ExpectedSuccess),
    TestDefinition(
      RequiredClaim.Public(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimAdmin)),
      expectedPermissionDenied,
    ),
    TestDefinition(RequiredClaim.Public(), ClaimSet.Claims.Admin, ExpectedSuccess),
    TestDefinition(
      RequiredClaim.Public(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimPublic)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.Public(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimIdentityProviderAdmin)),
      expectedPermissionDenied,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.Admin"

  List(
    TestDefinition(RequiredClaim.Admin(), ClaimSet.Claims.Empty, expectedPermissionDenied),
    TestDefinition(
      RequiredClaim.Admin(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimPublic)),
      expectedPermissionDenied,
    ),
    TestDefinition(RequiredClaim.Admin(), ClaimSet.Claims.Wildcard, ExpectedSuccess),
    TestDefinition(
      RequiredClaim.Admin(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimIdentityProviderAdmin)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.Admin(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimAdmin)),
      ExpectedSuccess,
    ),
    TestDefinition(RequiredClaim.Admin(), ClaimSet.Claims.Admin, ExpectedSuccess),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.AdminOrIdpAdmin"

  List(
    TestDefinition(
      RequiredClaim.AdminOrIdpAdmin(),
      ClaimSet.Claims.Empty,
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.AdminOrIdpAdmin(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimPublic)),
      expectedPermissionDenied,
    ),
    TestDefinition(RequiredClaim.AdminOrIdpAdmin(), ClaimSet.Claims.Wildcard, ExpectedSuccess),
    TestDefinition(
      RequiredClaim.AdminOrIdpAdmin(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimIdentityProviderAdmin)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.AdminOrIdpAdmin(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimAdmin)),
      ExpectedSuccess,
    ),
    TestDefinition(RequiredClaim.AdminOrIdpAdmin(), ClaimSet.Claims.Admin, ExpectedSuccess),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.ReadAs"

  List(
    TestDefinition(RequiredClaim.ReadAs("party"), ClaimSet.Claims.Empty, expectedPermissionDenied),
    TestDefinition(RequiredClaim.ReadAs("party"), ClaimSet.Claims.Wildcard, ExpectedSuccess),
    TestDefinition(RequiredClaim.ReadAs("party"), ClaimSet.Claims.Admin, expectedPermissionDenied),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimPublic)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsAnyParty)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party))),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party))),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party2))),
      expectedPermissionDenied,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.ExecuteAs"

  List(
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty,
      expectedPermissionDenied,
    ),
    TestDefinition(RequiredClaim.ExecuteAs("party"), ClaimSet.Claims.Wildcard, ExpectedSuccess),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Admin,
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimPublic)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsAnyParty)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party))),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party))),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ExecuteAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party2))),
      expectedPermissionDenied,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.ReadAsAnyParty"

  List(
    TestDefinition(RequiredClaim.ReadAsAnyParty(), ClaimSet.Claims.Empty, expectedPermissionDenied),
    TestDefinition(RequiredClaim.ReadAsAnyParty(), ClaimSet.Claims.Wildcard, ExpectedSuccess),
    TestDefinition(RequiredClaim.ReadAsAnyParty(), ClaimSet.Claims.Admin, expectedPermissionDenied),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimPublic)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsAnyParty)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ReadAsAnyParty(),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party2))),
      expectedPermissionDenied,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.ActAs"

  List(
    TestDefinition(RequiredClaim.ActAs("party"), ClaimSet.Claims.Empty, expectedPermissionDenied),
    TestDefinition(RequiredClaim.ActAs("party"), ClaimSet.Claims.Wildcard, ExpectedSuccess),
    TestDefinition(RequiredClaim.ActAs("party"), ClaimSet.Claims.Admin, expectedPermissionDenied),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimPublic)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsAnyParty)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsAnyParty)),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsAnyParty)),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party))),
      ExpectedSuccess,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimReadAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimExecuteAsParty(party2))),
      expectedPermissionDenied,
    ),
    TestDefinition(
      RequiredClaim.ActAs("party"),
      ClaimSet.Claims.Empty.copy(claims = Seq(ClaimActAsParty(party2))),
      expectedPermissionDenied,
    ),
  ).foreach(generateAuthorizationTest)

  val userId1 = "userId1"
  val userId2 = "userId2"

  def matchUserIdTestDef(
      claims: Seq[Claim],
      userId: Option[String],
      req: String,
      expectedResult: ExpectedResult,
      descSuffix: String,
      resolvedFromUser: Boolean = true,
  ): TestDefinition =
    TestDefinition(
      RequiredClaim.MatchUserIdForUserManagement(simpleLens),
      ClaimSet.Claims.Empty.copy(
        claims = claims,
        userId = userId,
        resolvedFromUser = resolvedFromUser,
      ),
      expectedResult,
      req = req,
      descSuffix = descSuffix,
      resultAssert = _ shouldBe userId1,
    )

  behavior of s"$className.authorize for RequiredClaim.MatchUserIdForUserManagement without Admin rights"

  List(
    matchUserIdTestDef(
      Nil,
      Some(userId1),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs match",
    ),
    matchUserIdTestDef(Nil, Some(userId1), "", ExpectedSuccess, "when missing request user ID"),
    matchUserIdTestDef(
      Nil,
      Some(userId2),
      userId1,
      expectedPermissionDenied,
      "when authorized and request user IDs don't match",
    ),
    matchUserIdTestDef(Nil, None, userId1, expectedInternal, "when undefined authorized user ID"),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.MatchUserIdForUserManagement with Admin rights"
  List(
    matchUserIdTestDef(
      Seq(ClaimAdmin),
      Some(userId1),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs match",
    ),
    matchUserIdTestDef(
      Seq(ClaimAdmin),
      Some(userId1),
      "",
      ExpectedSuccess,
      "when missing request user ID",
    ),
    matchUserIdTestDef(
      Seq(ClaimAdmin),
      Some(userId2),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
    ),
    matchUserIdTestDef(
      Seq(ClaimAdmin),
      None,
      userId1,
      expectedInternal,
      "when undefined authorized user ID",
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.MatchUserIdForUserManagement with IDP Admin rights"
  List(
    matchUserIdTestDef(
      Seq(ClaimIdentityProviderAdmin),
      Some(userId1),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs match",
    ),
    matchUserIdTestDef(
      Seq(ClaimIdentityProviderAdmin),
      Some(userId1),
      "",
      ExpectedSuccess,
      "when missing request user ID",
    ),
    matchUserIdTestDef(
      Seq(ClaimIdentityProviderAdmin),
      Some(userId2),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
    ),
    matchUserIdTestDef(
      Seq(ClaimIdentityProviderAdmin),
      None,
      userId1,
      expectedInternal,
      "when undefined authorized user ID",
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.MatchUserIdForUserManagement not resolvedFromUser"

  List(
    matchUserIdTestDef(
      Nil,
      Some(userId1),
      userId1,
      expectedPermissionDenied,
      "when authorized and request user IDs match",
      resolvedFromUser = false,
    ),
    matchUserIdTestDef(
      Nil,
      None,
      userId1,
      expectedPermissionDenied,
      "when undefined authenticated user ID",
      resolvedFromUser = false,
    ),
    matchUserIdTestDef(
      Nil,
      Some(userId2),
      userId1,
      expectedPermissionDenied,
      "when authorized and request user IDs don't match",
      resolvedFromUser = false,
    ),
    matchUserIdTestDef(
      Seq(ClaimAdmin),
      Some(userId1),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs match",
      resolvedFromUser = false,
    ),
    matchUserIdTestDef(
      Seq(ClaimAdmin),
      None,
      userId1,
      ExpectedSuccess,
      "when undefined authenticated user ID",
      resolvedFromUser = false,
    ),
    matchUserIdTestDef(
      Seq(ClaimAdmin),
      Some(userId2),
      userId1,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
      resolvedFromUser = false,
    ),
  ).foreach(generateAuthorizationTest)

  def anyAdminTestDef(
      claims: Seq[Claim],
      userId: Option[String],
      req: String,
      expectedResult: ExpectedResult,
      descSuffix: String,
      resolvedFromUser: Boolean = true,
  ): TestDefinition =
    TestDefinition(
      RequiredClaim.AdminOrIdpAdminOrSelfAdmin(simpleLens),
      ClaimSet.Claims.Empty.copy(
        claims = claims,
        userId = userId,
        resolvedFromUser = resolvedFromUser,
      ),
      expectedResult,
      req = req,
      descSuffix = descSuffix,
    )

  behavior of s"$className.authorize for AdminOrIdpAdminOrSelfAdmin with Admin claims"
  List(
    anyAdminTestDef(
      Seq(ClaimAdmin),
      Some(userId2),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs match",
    ),
    anyAdminTestDef(
      Seq(ClaimAdmin),
      Some(userId1),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
    ),
    anyAdminTestDef(
      Seq(ClaimAdmin),
      Some(userId1),
      "",
      ExpectedSuccess,
      "when request user ID missing",
    ),
    anyAdminTestDef(
      Seq(ClaimAdmin),
      None,
      userId2,
      ExpectedSuccess,
      "when authorized user ID missing",
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for AdminOrIdpAdminOrSelfAdmin with Idp Admin claims"
  List(
    anyAdminTestDef(
      Seq(ClaimIdentityProviderAdmin),
      Some(userId2),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs match",
    ),
    anyAdminTestDef(
      Seq(ClaimIdentityProviderAdmin),
      Some(userId1),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
    ),
    anyAdminTestDef(
      Seq(ClaimIdentityProviderAdmin),
      Some(userId1),
      "",
      ExpectedSuccess,
      "when request user ID missing",
    ),
    anyAdminTestDef(
      Seq(ClaimIdentityProviderAdmin),
      None,
      userId2,
      ExpectedSuccess,
      "when authorized user ID missing",
    ),
  )
    .foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for AdminOrIdpAdminOrSelfAdmin without Admin claims"
  List(
    anyAdminTestDef(
      Nil,
      Some(userId2),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs match",
    ),
    anyAdminTestDef(
      Nil,
      Some(userId1),
      userId2,
      expectedPermissionDenied,
      "when authorized and request user IDs don't match",
    ),
    anyAdminTestDef(
      Nil,
      Some(userId1),
      "",
      expectedPermissionDenied,
      "when request user ID missing",
    ),
    anyAdminTestDef(Nil, None, userId2, expectedInternal, "when authorized user ID missing"),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for AdminOrIdpAdminOrSelfAdmin with Admin claims when not resolvedFromUser"
  List(
    anyAdminTestDef(
      Seq(ClaimAdmin),
      Some(userId2),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs match",
      resolvedFromUser = false,
    ),
    anyAdminTestDef(
      Seq(ClaimAdmin),
      Some(userId1),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
      resolvedFromUser = false,
    ),
    anyAdminTestDef(
      Seq(ClaimAdmin),
      Some(userId1),
      "",
      ExpectedSuccess,
      "when request user ID missing",
      resolvedFromUser = false,
    ),
    anyAdminTestDef(
      Seq(ClaimAdmin),
      None,
      userId2,
      ExpectedSuccess,
      "when authorized user ID missing",
      resolvedFromUser = false,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for AdminOrIdpAdminOrSelfAdmin with Idp Admin claims when not resolvedFromUser"
  List(
    anyAdminTestDef(
      Seq(ClaimIdentityProviderAdmin),
      Some(userId2),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs match",
      resolvedFromUser = false,
    ),
    anyAdminTestDef(
      Seq(ClaimIdentityProviderAdmin),
      Some(userId1),
      userId2,
      ExpectedSuccess,
      "when authorized and request user IDs don't match",
      resolvedFromUser = false,
    ),
    anyAdminTestDef(
      Seq(ClaimIdentityProviderAdmin),
      Some(userId1),
      "",
      ExpectedSuccess,
      "when request user ID missing",
      resolvedFromUser = false,
    ),
    anyAdminTestDef(
      Seq(ClaimIdentityProviderAdmin),
      None,
      userId2,
      ExpectedSuccess,
      "when authorized user ID missing",
      resolvedFromUser = false,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for AdminOrIdpAdminOrSelfAdmin without Admin claims when not resolvedFromUser"
  List(
    anyAdminTestDef(
      Nil,
      Some(userId2),
      userId2,
      expectedPermissionDenied,
      "when authorized and request user IDs match",
      resolvedFromUser = false,
    ),
    anyAdminTestDef(
      Nil,
      Some(userId1),
      userId2,
      expectedPermissionDenied,
      "when authorized and request user IDs don't match",
      resolvedFromUser = false,
    ),
    anyAdminTestDef(
      Nil,
      Some(userId1),
      "",
      expectedPermissionDenied,
      "when request user ID missing",
      resolvedFromUser = false,
    ),
    anyAdminTestDef(
      Nil,
      None,
      userId2,
      expectedPermissionDenied,
      "when authorized user ID missing",
      resolvedFromUser = false,
    ),
  ).foreach(generateAuthorizationTest)

  behavior of s"$className.authorize for RequiredClaim.MatchUserId"

  it should "authorize for authenticated user ID matching request user ID" in {
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

  it should "authorize for authenticated user ID when request user ID is missing" in {
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

  it should "return permission denied for authenticated user ID not matching request user ID" in {
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

  it should "return permission denied for authenticated user ID not matching request user ID if skipUserIdValidationForAnyPartyReaders" in {
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

  it should "authorize for authenticated user ID not matching request user ID for AnyPartyReaders-s if skipUserIdValidationForAnyPartyReaders" in {
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

  it should "return invalid argument for no authenticated user ID, and no request user ID" in {
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

  def generateAuthorizationTest(td: TestDefinition): Unit = {
    val prettyClaims = s"Claims(${td.suppliedClaim.claims.map(_.toString).mkString(",")})"
    val testDescription = td.expectedResult match {
      case ExpectedSuccess =>
        s"authorize for $prettyClaims ${td.descSuffix}"
      case ExpectedFailure(code) =>
        s"return $code for $prettyClaims ${td.descSuffix}"
    }
    it should testDescription in {
      contextWithClaims(td.suppliedClaim) {
        authorizer().rpc(simpleReqRes)(td.requiredClaim)(td.req)
      }.map(td.resultAssert).transform {
        case Success(_) =>
          td.expectedResult match {
            case ExpectedSuccess =>
              Success(succeed)
            case ExpectedFailure(_) =>
              fail("Unexpected success")
          }
        case Failure(ex: StatusRuntimeException) =>
          td.expectedResult match {
            case ExpectedSuccess =>
              fail(s"Unexpected error ${ex.getStatus.getCode}")
            case ExpectedFailure(code) if code != ex.getStatus.getCode =>
              fail(s"Unexpected error ${ex.getStatus.getCode}")
            case _ => Success(succeed)
          }
        case ex => fail(s"Unexpected error $ex")
      }
    }
  }
}

object AuthorizerSpec {
  sealed trait ExpectedResult
  final case object ExpectedSuccess extends ExpectedResult
  final case class ExpectedFailure(code: Status.Code) extends ExpectedResult

  val expectedPermissionDenied: ExpectedFailure = ExpectedFailure(Status.PERMISSION_DENIED.getCode)
  val expectedInternal: ExpectedFailure = ExpectedFailure(Status.INTERNAL.getCode)

  val simpleRequest: String = "simpleRequest"
  val simpleReqRes: String => Future[String] = s => Future.successful(s)
  val simpleLens: Lens[String, String] = Lens[String, String](s => s)((_, s) => s)

  final case class TestDefinition(
      requiredClaim: RequiredClaim[String],
      suppliedClaim: ClaimSet.Claims,
      expectedResult: ExpectedResult,
      req: String = simpleRequest,
      descSuffix: String = "",
      resultAssert: String => Assertion = _ => succeed,
  )
}
