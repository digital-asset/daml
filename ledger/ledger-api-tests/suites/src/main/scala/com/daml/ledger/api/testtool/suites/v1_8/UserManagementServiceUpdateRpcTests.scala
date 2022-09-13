// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.v1.admin.user_management_service.{
  CreateUserRequest,
  UpdateUserRequest,
  UpdateUserResponse,
  User,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.google.protobuf.field_mask.FieldMask

trait UserManagementServiceUpdateRpcTests {
  self: UserManagementServiceIT =>

  userManagementTest(
    "UpdateAllUpdatableFields",
    "Update all updated fields",
    requiresUserExtensionsForHub = true,
  )(implicit ec => { ledger =>
    implicit val l: ParticipantTestContext = ledger
    for {
      // updating all fields 1 - verbose update paths
      _ <- withFreshUser(
        primaryParty = "primaryParty0",
        isDeactivated = true,
        annotations = Map("k1" -> "v1", "k2" -> "v2"),
      ) { user =>
        ledger.userManagement
          .updateUser(
            updateRequest(
              id = user.id,
              annotations = Map("k1" -> "v1a", "k3" -> "v3"),
              isDeactivated = false,
              primaryParty = "primaryParty1",
              updatePaths = Seq(
                "user.is_deactivated",
                "user.primary_party!replace",
                "user.metadata.annotations!merge",
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
                    annotations = Map("k1" -> "v1a", "k2" -> "v2", "k3" -> "v3"),
                  )
                )
              ),
            )
          }
      }
      // updating all fields 1 - compact update paths
      _ <- withFreshUser(
        primaryParty = "primaryParty0",
        isDeactivated = true,
        annotations = Map("k1" -> "v1", "k2" -> "v2"),
      ) { user =>
        ledger.userManagement
          .updateUser(
            updateRequest(
              id = user.id,
              annotations = Map("k1" -> "v1a", "k3" -> "v3"),
              isDeactivated = false,
              primaryParty = "primaryParty1",
              updatePaths = Seq(
                "user"
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
                    isDeactivated = true,
                    primaryParty = "primaryParty1",
                    annotations = Map("k1" -> "v1a", "k2" -> "v2", "k3" -> "v3"),
                  )
                )
              ),
            )
          }
      }
    } yield ()
  })

  userManagementTest(
    "UpdateUserEvenIfMetadataIsNotSetInUpdateRequest",
    "Update a user even if metadata is not set in the update request",
    requiresUserExtensionsForHub = true,
  )(implicit ec => { ledger =>
    val userId1 = ledger.nextUserId()
    for {
      _ <- ledger.createUser(CreateUserRequest(Some(newUser(userId1))))
      update1 <- ledger.userManagement
        .updateUser(
          UpdateUserRequest(
            user = Some(
              User(
                id = userId1,
                primaryParty = "nextPrimaryParty",
                metadata = None,
              )
            ),
            updateMask = Some(
              FieldMask(Seq("user.primary_party"))
            ),
          )
        )
    } yield assertEquals(
      "update with metadata not set in the request",
      unsetResourceVersion(update1),
      UpdateUserResponse(
        Some(
          newUser(
            userId1,
            primaryParty = "nextPrimaryParty",
          )
        )
      ),
    )
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
            updatePaths = Seq("user.metadata.annotations"),
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

  userManagementTest(
    "InvalidUpdateRequests",
    "Failing update requests",
    requiresUserExtensionsForHub = true,
  )(implicit ec => { ledger =>
    val userId = ledger.nextUserId()
    for {
      _ <- ledger.userManagement.createUser(
        CreateUserRequest(Some(newUser(id = userId, annotations = Map("k1" -> "v1", "k2" -> "v2"))))
      )
      // update 1 - unknown update modifier
      update1 <- ledger.userManagement
        .updateUser(
          updateRequest(
            id = userId,
            annotations = Map("k2" -> "v2"),
            updatePaths = Seq("user.metadata.annotations!badmodifer"),
          )
        )
        .mustFail("updating with an unknown update modifier")
      _ = assertGrpcError(
        update1,
        errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${userId}' failed due to: UnknownUpdateModifier"
        ),
      )

      // update 2 - unknown field path
      _ <- ledger.userManagement
        .updateUser(
          updateRequest(
            id = userId,
            annotations = Map("k2" -> "v2"),
            updatePaths = Seq("user.metadata.unknown_field"),
          )
        )
        .mustFailWith(
          "updating with an unknown update path 1",
          errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
          exceptionMessageSubstring = Some(
            s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${userId}' failed due to: UnknownFieldPath"
          ),
        )
      // update 3 - bad update path syntax: unknown field path
      _ <- ledger.userManagement
        .updateUser(
          updateRequest(
            id = userId,
            annotations = Map("k2" -> "v2"),
            updatePaths = Seq("user.dummy"),
          )
        )
        .mustFailWith(
          "update with an unknown update path 2 ",
          errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
          exceptionMessageSubstring = Some(
            s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${userId}' failed due to: UnknownFieldPath"
          ),
        )
      // update 4 - bad update path syntax
      _ <- ledger.userManagement
        .updateUser(
          updateRequest(
            id = userId,
            annotations = Map("k2" -> "v2"),
            updatePaths = Seq("aaa!bad!qwerty"),
          )
        )
        .mustFailWith(
          "update with an unknown update path",
          errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
          exceptionMessageSubstring = Some(
            s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${userId}' failed due to: InvalidUpdatePathSyntax"
          ),
        )
      // update 5 - no update paths
      _ <- ledger.userManagement
        .updateUser(
          updateRequest(
            id = userId,
            annotations = Map("k2" -> "v2"),
            updatePaths = Seq.empty,
          )
        )
        .mustFailWith(
          "update with an unknown update path",
          errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
          exceptionMessageSubstring = Some(
            s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${userId}' failed due to: EmptyFieldMask"
          ),
        )
      // update 6 - invalid syntax for user id
      _ <- ledger.userManagement
        .updateUser(
          updateRequest(
            id = "%%!!!",
            annotations = Map("k2" -> "v2"),
            updatePaths = Seq.empty,
          )
        )
        .mustFailWith(
          "update with an unknown update path",
          errorCode = LedgerApiErrors.RequestValidation.InvalidField,
          exceptionMessageSubstring = Some(
            s"INVALID_ARGUMENT: INVALID_FIELD(8,0): The submitted command has a field with invalid value: Invalid field user.id: User ID \"%%!!!\" does not match regex \"[a-z0-9@^$$.!`\\-#+'~_|:]{1,128}\""
          ),
        )
    } yield ()
  })

}
