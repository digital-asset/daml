// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.user_management_service.UpdateUserRequest
import com.google.protobuf.field_mask.FieldMask

import scala.concurrent.Future

final class UpdateUserAuthIT extends AdminServiceCallAuthTests with UserManagementAuth {

  override def serviceCallName: String = "UserManagementService#UpdateUser"

  override def serviceCallWithToken(token: Option[String]): Future[Any] = {
    for {
      response <- createFreshUser(token)
      _ <- stub(token).updateUser(
        UpdateUserRequest(
          user = response.user,
          updateMask = Some(FieldMask(scala.Seq("is_deactivated"))),
        )
      )
    } yield ()
  }

}
