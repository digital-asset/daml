// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.v1.admin.user_management_service.{
  UpdateUserRequest,
  UpdateUserResponse,
  User,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.google.protobuf.field_mask.FieldMask

trait UserManagementServiceUpdateRpcTests {
  self: UserManagementServiceIT =>

  userManagementTestWithFreshUser(
    "AllowSpecifyingNonModifiableFieldsIfTheyAreNotAnUpdate",
    "Allow to specify non modifiable fields in the update mask",
  )()(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          updatePaths = Seq("id", "metadata.resource_version"),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating a user",
          unsetResourceVersion(updateResp),
          UpdateUserResponse(
            Some(unsetResourceVersion(user))
          ),
        )

      }

  })

  userManagementTestWithFreshUser(
    "UpdateAllUpdatableFields",
    "Update all updatable fields",
  )(
    primaryParty = "primaryParty0",
    isDeactivated = true,
    annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"),
  )(implicit ec => { (ledger, user) =>
    // updating all fields 1 - verbose update paths
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k1" -> "v1a", "k3" -> "", "k4" -> "v4"),
          isDeactivated = false,
          primaryParty = "primaryParty1",
          updatePaths = Seq(
            "is_deactivated",
            "primary_party",
            "metadata",
          ),
        )
      )
      .map { updateResp =>
        assertEquals(
          "updating user 1",
          unsetResourceVersion(updateResp),
          UpdateUserResponse(
            Some(
              newUser(
                id = user.id,
                isDeactivated = false,
                primaryParty = "primaryParty1",
                annotations = Map("k1" -> "v1a", "k2" -> "v2", "k4" -> "v4"),
              )
            )
          ),
        )
      }
  })

  userManagementTestWithFreshUser(
    "UpdateUserEvenIfMetadataIsNotSetInUpdateRequest",
    "Update a user even if metadata field is not set in the update request",
  )()(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        UpdateUserRequest(
          user = Some(
            User(
              id = user.id,
              primaryParty = "nextPrimaryParty",
              metadata = None,
            )
          ),
          updateMask = Some(FieldMask(Seq("primary_party"))),
        )
      )
      .map { updateResp =>
        assertEquals(
          "update with metadata not set in the request",
          unsetResourceVersion(updateResp),
          UpdateUserResponse(Some(newUser(user.id, primaryParty = "nextPrimaryParty"))),
        )
      }
  })

  userManagementTest(
    "FailUpdateNonExistentUser",
    "Fail when attempting to update a non-existent user",
    requiresUserExtensionsForHub = true,
  )(implicit ec => { ledger =>
    val userId1 = ledger.nextUserId()
    for {
      _ <- ledger.userManagement
        .updateUser(
          updateRequest(
            id = userId1,
            annotations = Map("k1" -> "v1"),
            updatePaths = Seq("metadata.annotations"),
          )
        )
        .mustFailWith(
          "updating non-existent party",
          errorCode = LedgerApiErrors.Admin.UserManagement.UserNotFound,
          exceptionMessageSubstring = Some(
            s"NOT_FOUND: USER_NOT_FOUND(11,0): updating user failed for unknown user \"$userId1\""
          ),
        )
    } yield ()
  })

}
