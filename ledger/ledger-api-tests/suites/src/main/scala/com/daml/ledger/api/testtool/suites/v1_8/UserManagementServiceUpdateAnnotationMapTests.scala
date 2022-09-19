// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Assertions.assertEquals
import com.daml.ledger.api.testtool.infrastructure.Assertions._

trait UserManagementServiceUpdateAnnotationMapTests {
  self: UserManagementServiceUpdateRpcTests with UserManagementServiceIT =>

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithNoUpdateModifierWithNonEmptyMapAndExactUpdatePath",
    "Update annotations using update paths with no update modifiers with a non-empty map and the exact update path (resulting in a successful merge update)",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k1" -> "v1a", "k3" -> "v3"),
          updatePaths = Seq("user.metadata.annotations"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating annotations 1 ",
          extractUpdatedAnnotations(updateResp),
          Map("k1" -> "v1a", "k2" -> "v2", "k3" -> "v3"),
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithNoUpdateModifierWithNonEmptyMapAndShorthandUpdatePath",
    "Update annotations using update paths with no update modifiers with a non-empty map and a shorthand update path (resulting in a successful merge update)",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k1" -> "v1b", "k3" -> "v3"),
          updatePaths = Seq("user"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating annotations 2",
          extractUpdatedAnnotations(updateResp),
          Map("k1" -> "v1b", "k2" -> "v2", "k3" -> "v3"),
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithNoUpdateModifierWithEmptyMapAndExactUpdatePath",
    "Update annotations using update paths with no update modifiers with the empty map and the the exact update path (resulting in a successful replace update)",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map.empty,
          updatePaths = Seq("user.metadata.annotations"),
        )
      )
      .map { responseResp =>
        assertEquals(
          "updating annotations 3",
          extractUpdatedAnnotations(responseResp),
          Map.empty,
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithNoUpdateModifierWithEmptyMapAndShorthandUpdatePath",
    "Update annotations using update paths with no update modifiers with the empty map and a shorthand update path (resulting in a failed update as the update request is a no-op update)",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map.empty,
          updatePaths = Seq("user"),
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
    "UpdateAnnotationsWithExplicitMergeUpdateModifierWithNonEmptyMapAndExactUpdatePath",
    "Update annotations using update paths with explicit '!merge' update modifier with a non-empty map and the exact update path (resulting in a successful merge update)",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k1" -> "v1a", "k3" -> "v3"),
          updatePaths = Seq("user.metadata.annotations!merge"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating annotations 1 ",
          extractUpdatedAnnotations(updateResp),
          Map("k1" -> "v1a", "k2" -> "v2", "k3" -> "v3"),
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithExplicitMergeUpdateModifierWithNonEmptyMapAndShorthandUpdatePath",
    "Update annotations using update paths with explicit '!merge' update modifier with a non-empty map and a shorthand update path (resulting in a successful merge update)",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k1" -> "v1b", "k3" -> "v3"),
          updatePaths = Seq("user!merge"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating annotations 2",
          extractUpdatedAnnotations(updateResp),
          Map("k1" -> "v1b", "k2" -> "v2", "k3" -> "v3"),
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithExplicitMergeUpdateModifierWithEmptyMapAndExactUpdatePath",
    "Update annotations using update paths with explicit '!merge' update modifier with the empty map and the exact update path " +
      "(resulting in a failed update request as explicit merge on an exact field path with a default value is invalid)",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map.empty,
          updatePaths = Seq("user.metadata.annotations!merge"),
        )
      )
      .mustFailWith(
        "explicit merge update on the exact annotations field path",
        errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${user.id}' failed due to: The update path: 'user.metadata.annotations!merge' for a map field contains an explicit '!merge' modifier but the new value is an empty map. This would result in a no-up update for this field and is probably a mistake."
        ),
      )
  })

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithExplicitMergeUpdateModifierWithEmptyMapAndShorthandUpdatePath",
    "Update annotations using update paths with explicit '!merge' update modifier with the empty map and a shorthand update path (resulting in a failed update as the update request is a no-op update)",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map.empty,
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
    "UpdateAnnotationsWithExplicitReplaceUpdateModifierWithNonEmptyMapAndExactUpdatePath",
    "Update annotations using update paths with explicit '!replace' update modifier with a non-empty map and the exact update path (resulting in a successful replace update)",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k1" -> "v1a", "k3" -> "v3"),
          updatePaths = Seq("user.metadata.annotations!replace"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating annotations 1",
          extractUpdatedAnnotations(updateResp),
          Map("k1" -> "v1a", "k3" -> "v3"),
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithExplicitReplaceUpdateModifierWithNonEmptyMapAndShorthandUpdatePath",
    "Update annotations using update paths with explicit '!replace' update modifier with a non-empty map and a shorthand update path (resulting in a successful replace update)",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k1" -> "v1b", "k3" -> "v3"),
          updatePaths = Seq("user!replace"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating annotations 2",
          extractUpdatedAnnotations(updateResp),
          Map("k1" -> "v1b", "k3" -> "v3"),
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithExplicitReplaceUpdateModifierWithEmptyMapAndExactUpdatePath",
    "Update annotations using update paths with explicit '!replace' update modifier with the empty map and the the exact update path (resulting in a successful replace update)",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map.empty,
          updatePaths = Seq("user.metadata.annotations!replace"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating annotations 3",
          extractUpdatedAnnotations(updateResp),
          Map.empty,
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdateAnnotationsWithExplicitReplaceUpdateModifierWithEmptyMapAndShorthandUpdatePath",
    "Update annotations using update paths with explicit '!replace' update modifier with the empty map and a shorthand update path (resulting in a successful replace update)",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2"))(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map.empty,
          updatePaths = Seq("user!replace"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating annotations 4",
          extractUpdatedAnnotations(updateResp),
          Map.empty,
        )
      }
  })

}
