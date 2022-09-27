// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Assertions._

trait UserManagementServiceUpdatePrimitivePropertiesTests {
  self: UserManagementServiceIT =>

  testWithFreshUser(
    "UpdatePrimaryPartyUsingNonEmptyValue",
    "Update primary party using a non-empty value",
  )(primaryParty = "primaryParty0")(implicit ec =>
    ledger =>
      user =>
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
              "updating primary party",
              extractUpdatedPrimaryParty(updateResp),
              expected = "primaryParty1",
            )
          }
  )

  testWithFreshUser(
    "UpdatePrimaryPartyUsingEmptyValue",
    "Update primary party using the empty value",
  )(primaryParty = "primaryParty0")(implicit ec =>
    ledger =>
      user =>
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
  )

  testWithFreshUser(
    "UpdateIsDeactivatedUsingNonDefaultValue",
    "Update primary party using a non default value",
  )(isDeactivated = false)(implicit ec =>
    ledger =>
      user =>
        ledger.userManagement
          .updateUser(
            updateRequest(
              id = user.id,
              isDeactivated = true,
              updatePaths = Seq("is_deactivated"),
            )
          )
          .map { updateResp =>
            assertEquals(
              "updating is_deactivated",
              extractIsDeactivated(updateResp),
              expected = true,
            )
          }
  )

  testWithFreshUser(
    "UpdateIsDeactivatedUsingTheDefaultValue",
    "Update primary party using the default value",
  )(isDeactivated = true)(implicit ec =>
    ledger =>
      user =>
        ledger.userManagement
          .updateUser(
            updateRequest(
              id = user.id,
              isDeactivated = false,
              updatePaths = Seq("is_deactivated"),
            )
          )
          .map { updateResp =>
            assertEquals(
              "updating is_deactivated",
              extractIsDeactivated(updateResp),
              expected = false,
            )
          }
  )

}
