// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.nio.charset.StandardCharsets
import java.util.Base64

import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{DamlContextualizedErrorLogger, ErrorsAssertions}
import com.daml.ledger.api.domain.User
import com.daml.ledger.participant.state.index.v2.UserManagementStore.UsersPage
import com.daml.lf.data.Ref
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ApiUserManagementServiceSpec
    extends AnyFlatSpec
    with Matchers
    with EitherValues
    with ErrorsAssertions {

  private val errorLogger = DamlContextualizedErrorLogger.forTesting(getClass)

  it should "test users page token encoding and decoding" in {
    val id2 = Ref.UserId.assertFromString("user2")
    val page = UsersPage(
      users = Seq(User(Ref.UserId.assertFromString("user1"), None), User(id2, None))
    )
    val actualNextPageToken = ApiUserManagementService.encodeNextPageToken(page)
    actualNextPageToken shouldBe "dXNlcjI="
    ApiUserManagementService.decodePageToken(actualNextPageToken)(errorLogger) shouldBe Right(
      Some(id2)
    )
  }

  it should "test users page token encoding and decoding for an empty page" in {
    val page = UsersPage(users = Seq.empty)
    val actualNextPageToken = ApiUserManagementService.encodeNextPageToken(page)
    actualNextPageToken shouldBe ("")
    ApiUserManagementService.decodePageToken(actualNextPageToken)(errorLogger) shouldBe Right(None)
  }

  it should "return invalid argument error when token is not a base64" in {
    val actualNextPageToken =
      ApiUserManagementService.decodePageToken("not-a-base64-string!!")(errorLogger)
    val error = actualNextPageToken.left.value
    assertError(
      actual = error,
      expectedF = LedgerApiErrors.RequestValidation.InvalidArgument
        .Reject("Invalid page token")(_)
        .asGrpcError,
    )
  }

  it should "return invalid argument error when token is base64 but not a valid user id string" in {
    val notValidUserId = "not a valid user id"
    Ref.UserId.fromString(notValidUserId).isLeft shouldBe true
    val badPageToken = new String(
      Base64.getEncoder.encode(notValidUserId.getBytes(StandardCharsets.UTF_8)),
      StandardCharsets.UTF_8,
    )

    val actualNextPageToken = ApiUserManagementService.decodePageToken(badPageToken)(errorLogger)
    val error = actualNextPageToken.left.value
    assertError(
      actual = error,
      expectedF = LedgerApiErrors.RequestValidation.InvalidArgument
        .Reject("Invalid page token")(_)
        .asGrpcError,
    )
  }
}
