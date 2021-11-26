// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v1.admin.user_management_service.{
  CreateUserRequest,
  DeleteUserRequest,
  GetUserRequest,
  GrantUserRightsRequest,
  ListUserRightsRequest,
  RevokeUserRightsRequest,
  User,
  Right => Permission,
}

final class UserManagementServiceIT extends LedgerTestSuite {
  test(
    "AllRpcs_SuccessAndFail",
    "Exercise every rpc once with success and once with a failure",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      // TODO: actually exercise all RPCs
      createResult <- ledger.userManagement.createUser(CreateUserRequest(Some(User("a", "b")), Nil))
      getUserResult <- ledger.userManagement.getUser(GetUserRequest("b"))
      grantResult <- ledger.userManagement.grantUserRights(
        GrantUserRightsRequest(
          "a",
          List(Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))),
        )
      )
      listRightsResult <- ledger.userManagement.listUserRights(ListUserRightsRequest("a"))
      revokeResult <- ledger.userManagement.revokeUserRights(
        RevokeUserRightsRequest(
          "a",
          List(Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))),
        )
      )
      _ <- ledger.userManagement.deleteUser(DeleteUserRequest("a"))
    } yield {
      assert(createResult == User("a", "b"))
      assert(getUserResult == User("a", "b"))
      assert(
        grantResult.newlyGrantedRights == List(
          Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))
        )
      )
      assert(
        revokeResult.newlyRevokedRights == List(
          Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))
        )
      )
      assert(
        listRightsResult.rights.toSet == Set(
          Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))
//          Permission(Permission.Kind.CanActAs(Permission.CanActAs("acting-party"))),
//          Permission(Permission.Kind.CanReadAs(Permission.CanReadAs("reader-party"))),
        )
      )
    }
  })
}
