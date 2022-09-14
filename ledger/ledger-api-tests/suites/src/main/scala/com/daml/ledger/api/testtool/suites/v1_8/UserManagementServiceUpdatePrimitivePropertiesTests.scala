// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Assertions._

// Test updating a primitive fields taking primaryParty as a representative example of such a field.
trait UserManagementServiceUpdatePrimitivePropertiesTests {
  self: UserManagementServiceIT =>

  userManagementTestWithFreshUser(
    "UpdatePrimaryPartyWithNoUpdateModifierWithNonEmptyPrimaryPartyAndExactUpdatePath",
    "Update primary party using update paths with no update modifiers with a non-empty primary party and the exact update path (resulting in a successful update)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          primaryParty = "primaryParty1",
          updatePaths = Seq("user.primary_party"),
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
    "UpdatePrimaryPartyWithNoUpdateModifierWithNonEmptyPrimaryPartyAndShorthandUpdatePath",
    "Update primary party using update paths with no update modifiers with a non-empty primary party and a shorthand update path (resulting in a successful update)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(id = user.id, primaryParty = "primaryParty2", updatePaths = Seq("user"))
      )
      .map { updateResp =>
        assertEquals(
          "updating primary party 2",
          extractUpdatedPrimaryParty(updateResp),
          expected = "primaryParty2",
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdatePrimaryPartyWithNoUpdateModifierWithEmptyPrimaryPartyAndExactUpdatePath",
    "Update primary party using update paths with no update modifiers with the empty primary party and the the exact update path (resulting in a successful update)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(id = user.id, primaryParty = "", updatePaths = Seq("user.primary_party"))
      )
      .map { updateResp =>
        assertEquals(
          "updating primary party 3",
          extractUpdatedPrimaryParty(updateResp),
          expected = "",
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdatePrimaryPartyWithNoUpdateModifierWithEmptyPrimaryPartyAndShorthandUpdatePath",
    "Update primary party using update paths with no update modifiers with the empty primary party and a shorthand update path (resulting in a failed update as the update request is a no-op update)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(updateRequest(id = user.id, primaryParty = "", updatePaths = Seq("user")))
      .mustFailWith(
        "empty update fail",
        errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${user.id}' failed due to: Update request describes a no-up update"
        ),
      )
  })

  userManagementTestWithFreshUser(
    "UpdatePrimaryPartyWithExplicitMergeUpdateModifierWithNonEmptyPrimaryPartyAndExactUpdatePath",
    "Update primary using update paths with explicit '!merge' update modifier with a non-empty primary party and the exact update path (resulting in a failed update as the explicit merge modifier on an exact field paths is invalid)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          primaryParty = "primaryParty1",
          updatePaths = Seq("user.primary_party!merge"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating annotations 1",
          extractUpdatedPrimaryParty(updateResp),
          "primaryParty1",
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdatePrimaryPartyWithExplicitMergeUpdateModifierWithNonEmptyPrimaryPartyAndShorthandUpdatePath",
    "Update primary using update paths with explicit '!merge' update modifier with a non-empty map and a shorthand update path (resulting in a successful update)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          primaryParty = "primaryParty2",
          updatePaths = Seq("user!merge"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating annotations 2",
          extractUpdatedPrimaryParty(updateResp),
          "primaryParty2",
        )

      }
  })

  userManagementTestWithFreshUser(
    "UpdatePrimaryPartyWithExplicitMergeUpdateModifierWithEmptyPrimaryPartyAndExactUpdatePath",
    "Update primary using update paths with explicit '!merge' update modifier  with the empty map and the exact update path (resulting in a failed update as the explicit merge modifier on an exact field paths is invalid)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          primaryParty = "",
          updatePaths = Seq("user.primary_party!merge"),
        )
      )
      .mustFailWith(
        "explicit merge update on the exact primary_party field path",
        errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${user.id}' failed due to: The update path: 'user.primary_party!merge' for a primitive field contains an explicit '!merge' modifier but the new value is the default value. This would result in a no-up update for this field and is probably a mistake."
        ),
      )
  })

  userManagementTestWithFreshUser(
    "UpdatePrimaryPartyWithExplicitMergeUpdateModifierWithEmptyPrimaryPartyAndShorthandUpdatePath",
    "Update primary using update paths with explicit '!merge' update modifier with the empty map and a shorthand update path (resulting in a failed update as the update request is a no-op update)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          primaryParty = "",
          updatePaths = Seq("user!merge"),
        )
      )
      .mustFailWith(
        "empty update fail",
        errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${user.id}' failed due to: Update request describes a no-up update"
        ),
      )
  })

  userManagementTestWithFreshUser(
    "UpdatePrimaryPartyWithExplicitReplaceUpdateModifierWithNonEmptyPrimaryPartyAndExactUpdatePath",
    "Update primary party using update paths with explicit '!replace' update modifier with a non-empty primary party and the exact update path (resulting in a successful update)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          primaryParty = "primaryParty1",
          updatePaths = Seq("user.primary_party!replace"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating primary party 1",
          extractUpdatedPrimaryParty(updateResp),
          expected = "primaryParty1",
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdatePrimaryPartyWithExplicitReplaceUpdateModifierWithNonEmptyPrimaryPartyAndShorthandUpdatePath",
    "Update primary party using update paths with explicit '!replace' update modifier with a non-empty primary party and a shorthand update path (resulting in a successful update)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          primaryParty = "primaryParty2",
          updatePaths = Seq("user!replace"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating primary party 2",
          extractUpdatedPrimaryParty(updateResp),
          expected = "primaryParty2",
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdatePrimaryPartyWithExplicitReplaceUpdateModifierWithEmptyPrimaryPartyAndExactUpdatePath",
    "Update primary party using update paths with explicit '!replace' update modifier with the empty primary party and the the exact update path (resulting in a successful update)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          primaryParty = "",
          updatePaths = Seq("user.primary_party!replace"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating primary party 3",
          extractUpdatedPrimaryParty(updateResp),
          "",
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdatePrimaryPartyWithExplicitReplaceUpdateModifierWithEmptyPrimaryPartyAndShorthandUpdatePath",
    "Update primary party using update paths with explicit '!replace' update modifier with the empty primary party and a shorthand update path (resulting in a successful replace update)",
  )(primaryParty = "primaryParty0")(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          primaryParty = "",
          updatePaths = Seq("user!replace"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating primary party 4",
          extractUpdatedPrimaryParty(updateResp),
          "",
        )
      }
  })

}
