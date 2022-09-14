// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.v1.admin.user_management_service.UpdateUserRequest
import com.google.protobuf.field_mask.FieldMask
import com.daml.ledger.api.testtool.infrastructure.Assertions._

trait UserManagementServiceInvalidUpdateRequestTests {
  self: UserManagementServiceIT =>

  userManagementTestWithFreshUser(
    "InvalidUpdateRequestsUnknownUpdateModifier",
    "Failing update requests when then update mask contains an unknown update modifier",
  )()(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k2" -> "v2"),
          updatePaths = Seq("user.metadata.annotations!badmodifer"),
        )
      )
      .mustFailWith(
        "updating with an unknown update modifier",
        errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${user.id}' failed due to: The update path: 'user.metadata.annotations!badmodifer' contains an unknown update modifier."
        ),
      )
  })

  userManagementTestWithFreshUser(
    "InvalidUpdateRequestsUnknownFieldPath",
    "Failing update requests when the update mask contains a path to an unknown field",
  )()(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k2" -> "v2"),
          updatePaths = Seq("user.metadata.unknown_field"),
        )
      )
      .mustFailWith(
        "updating with an unknown update path 1",
        errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${user.id}' failed due to: The update path: 'user.metadata.unknown_field' points to an unknown or an unmodifiable field."
        ),
      )
  })

  userManagementTestWithFreshUser(
    "InvalidUpdateRequestsSyntaxError",
    "Failing update requests when the update mask contains a path with invalid syntax",
  )()(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k2" -> "v2"),
          updatePaths = Seq("aaa!bad!qwerty"),
        )
      )
      .mustFailWith(
        "update with an unknown update path",
        errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${user.id}' failed due to: The update path: 'aaa!bad!qwerty' has invalid syntax."
        ),
      )
  })

  userManagementTestWithFreshUser(
    "InvalidUpdateRequestsNoUpdatePaths",
    "Failing update requests when the update mask is empty",
  )()(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k2" -> "v2"),
          updatePaths = Seq.empty,
        )
      )
      .mustFailWith(
        "update with an unknown update path",
        errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${user.id}' failed due to: The update mask contains no entries"
        ),
      )
  })

  userManagementTestWithFreshUser(
    "InvalidUpdateRequestsInvalidUserIdSyntax",
    "Failing update requests when user id is not a valid user id",
  )()(implicit ec => { (ledger, _) =>
    ledger.userManagement
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
  })

  userManagementTestWithFreshUser(
    "InvalidUpdateRequestsUpdatingUnmodifiableField",
    "Failing update requests when attempting to update an unmodifiable field",
  )()(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          updatePaths = Seq("user.id"),
        )
      )
      .mustFailWith(
        "update with an unknown update path",
        errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${user.id}' failed due to: The update path: 'user.id' points to an unknown or an unmodifiable field."
        ),
      )
  })

  userManagementTestWithFreshUser(
    "InvalidUpdateRequestsEmptyUpdatePath",
    "Failing update requests when the update mask contains an empty update path",
  )()(implicit ec => { (ledger, user) =>
    ledger.userManagement
      .updateUser(
        updateRequest(
          id = user.id,
          annotations = Map("k2" -> "v2"),
          updatePaths = Seq(""),
        )
      )
      .mustFailWith(
        "update with an unknown update path",
        errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUpdateUserRequest,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_USER_UPDATE_REQUEST(8,0): Update operation for user id '${user.id}' failed due to: The update path: '' has invalid syntax."
        ),
      )
  })

  userManagementTestWithFreshUser(
    "InvalidUpdateRequestsUserFieldIsUnset",
    "Failing update requests when user field is unset",
  )()(implicit ec => { (ledger, _) =>
    ledger.userManagement
      .updateUser(
        UpdateUserRequest(
          user = None,
          updateMask = Some(FieldMask(Seq("user"))),
        )
      )
      .mustFailWith(
        "update with an unknown update path",
        errorCode = LedgerApiErrors.RequestValidation.MissingField,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: MISSING_FIELD(8,0): The submitted command is missing a mandatory field: user"
        ),
      )
  })

  // TODO um-for-hub: Make sure party update call tests cover the same cases if relevant
}
