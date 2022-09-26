// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Assertions._

// Test updating a primitive fields taking primaryParty as a representative example of such a field.
trait UserManagementServiceUpdatePrimitivePropertiesTests {
  self: UserManagementServiceIT =>

  userManagementTestWithFreshUser(
    "UpdatePrimaryPartyWithNonEmptyPrimaryParty",
    "Update primary party using update paths with a non-empty primary party and the exact update path (resulting in a successful update)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          primaryParty = "primaryParty1",
          updatePaths = Seq("primary_party"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating primary party 1 ",
          extractUpdatedPrimaryParty(updateResp),
          expected = "primaryParty1",
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdatePrimaryPartyWithEmptyPrimaryParty",
    "Update primary party using update paths with the empty primary party and the the exact update path (resulting in a successful update)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(id = user.id, primaryParty = "", updatePaths = Seq("primary_party"))
      )
      .map { updateResp =>
        assertEquals(
          "updating primary party 3",
          extractUpdatedPrimaryParty(updateResp),
          expected = "",
        )
      }
  })

}
