// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import java.util.UUID

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v1.admin.user_management_service.{
  CreateUserRequest,
  DeleteUserRequest,
  GrantUserRightsRequest,
  ListUserRightsRequest,
  User,
  Right => Permission,
}
import io.grpc.Status

import scala.collection.immutable.Iterable

final class UserManagementLimitsIT extends LedgerTestSuite {

  def assertSameElements[T](actual: Iterable[T], expected: Iterable[T]): Unit = {
    assert(
      actual.toSet == expected.toSet,
      s"Actual ${actual.mkString(", ")} should have the same elements as (expected): ${expected.mkString(", ")}",
    )
  }

  def assertEquals(actual: Any, expected: Any): Unit = {
    assert(actual == expected, s"Actual |${actual}| should be equal (expected): |${expected}|")
  }

  test(
    "UserManagementUserRightsLimit",
    "Test user rights per user limit",
    allocate(NoParties),
    enabled = _.userManagement,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    def assertTooManyUserRightsError(t: Throwable): Unit = {
      assertGrpcError(
        participant = ledger,
        t = t,
        expectedCode = Status.Code.FAILED_PRECONDITION,
        selfServiceErrorCode = LedgerApiErrors.AdminServices.TooManyUserRights,
        exceptionMessageSubstring = None,
      )
    }

    def createCanActAs(id: Int) =
      Permission(Permission.Kind.CanActAs(Permission.CanActAs(s"acting-party-$id")))

    val user1 = User(UUID.randomUUID.toString, "")
    val user2 = User(UUID.randomUUID.toString, "")

    for {
      maxNumberOfUserRightsPerUser <- ledger.maxNumberOfUserRightsPerUser.map(
        _.getOrElse(fail("Could not read user rights limit"))
      )
      permissionsMaxAndOne = (1 to (maxNumberOfUserRightsPerUser + 1)).map(createCanActAs)
      permissionOne = permissionsMaxAndOne.head
      permissionsMax = permissionsMaxAndOne.tail
      // cannot create user with #limit+1 rights
      create1 <- ledger.userManagement
        .createUser(CreateUserRequest(Some(user1), permissionsMaxAndOne))
        .mustFail(
          "creating user with too many rights"
        )
      // can create user with #limit rights
      create2 <- ledger.userManagement.createUser(CreateUserRequest(Some(user1), permissionsMax))
      // fails adding one more right
      grant1 <- ledger.userManagement
        .grantUserRights(GrantUserRightsRequest(user1.id, rights = Seq(permissionOne)))
        .mustFail(
          "granting more rights exceeds max number of user rights per user"
        )
      // rights already added are intact
      rights1 <- ledger.userManagement.listUserRights(ListUserRightsRequest(user1.id))
      // can create other users with #limit rights
      create3 <- ledger.userManagement.createUser(CreateUserRequest(Some(user2), permissionsMax))
      // cleanup
      _ <- ledger.userManagement.deleteUser(DeleteUserRequest(user1.id))
      _ <- ledger.userManagement.deleteUser(DeleteUserRequest(user2.id))

    } yield {
      assertTooManyUserRightsError(create1)
      assertEquals(create2, user1)
      assertTooManyUserRightsError(grant1)
      assertEquals(rights1.rights.size, permissionsMaxAndOne.tail.size)
      assertSameElements(rights1.rights, permissionsMaxAndOne.tail)
      assertEquals(create3, user2)
    }
  })

}
