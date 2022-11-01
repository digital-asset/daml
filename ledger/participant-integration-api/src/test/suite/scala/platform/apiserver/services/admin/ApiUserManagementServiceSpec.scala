// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.nio.charset.StandardCharsets
import java.util.Base64
import com.daml.error.{DamlContextualizedErrorLogger, ErrorsAssertions}
import com.daml.platform.apiserver.page_tokens.ListUsersPageTokenPayload
import com.daml.lf.data.Ref
import com.daml.platform.error.definitions.LedgerApiErrors
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
    val actualNextPageToken = ApiUserManagementService.encodeNextPageToken(Some(id2))
    actualNextPageToken shouldBe "CgV1c2VyMg=="
    ApiUserManagementService.decodeUserIdFromPageToken(actualNextPageToken)(
      errorLogger
    ) shouldBe Right(
      Some(id2)
    )
  }

  it should "test users empty page token encoding and decoding" in {
    val actualNextPageToken = ApiUserManagementService.encodeNextPageToken(None)
    actualNextPageToken shouldBe ("")
    ApiUserManagementService.decodeUserIdFromPageToken(actualNextPageToken)(
      errorLogger
    ) shouldBe Right(None)
  }

  it should "return invalid argument error when token is not a base64" in {
    val actualNextPageToken =
      ApiUserManagementService.decodeUserIdFromPageToken("not-a-base64-string!!")(errorLogger)
    val error = actualNextPageToken.left.value
    assertError(
      actual = error,
      expectedF = LedgerApiErrors.RequestValidation.InvalidArgument
        .Reject("Invalid page token")(_)
        .asGrpcError,
    )
  }

  it should "return invalid argument error when token is base64 but not a valid protobuf" in {
    val notValidProtoBufBytes = "not a valid proto buf".getBytes()
    val badPageToken = new String(
      Base64.getEncoder.encode(notValidProtoBufBytes),
      StandardCharsets.UTF_8,
    )

    val actualNextPageToken =
      ApiUserManagementService.decodeUserIdFromPageToken(badPageToken)(errorLogger)
    val error = actualNextPageToken.left.value
    assertError(
      actual = error,
      expectedF = LedgerApiErrors.RequestValidation.InvalidArgument
        .Reject("Invalid page token")(_)
        .asGrpcError,
    )
  }

  it should "return invalid argument error when token is valid base64 encoded protobuf but does not contain a valid user id string" in {
    val notValidUserId = "not a valid user id"
    Ref.UserId.fromString(notValidUserId).isLeft shouldBe true
    val payload = ListUsersPageTokenPayload(
      userIdLowerBoundExcl = notValidUserId
    )
    val badPageToken = new String(
      Base64.getEncoder.encode(payload.toByteArray),
      StandardCharsets.UTF_8,
    )

    val actualNextPageToken =
      ApiUserManagementService.decodeUserIdFromPageToken(badPageToken)(errorLogger)
    val error = actualNextPageToken.left.value
    assertError(
      actual = error,
      expectedF = LedgerApiErrors.RequestValidation.InvalidArgument
        .Reject("Invalid page token")(_)
        .asGrpcError,
    )
  }
}
