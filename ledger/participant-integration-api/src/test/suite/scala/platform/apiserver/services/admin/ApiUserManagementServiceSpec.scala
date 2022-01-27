// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.domain.User
import com.daml.ledger.participant.state.index.v2.UserManagementStore.UsersPage
import com.daml.lf.data.Ref
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ApiUserManagementServiceSpec extends AnyFlatSpec with Matchers {

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

}
