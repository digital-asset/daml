// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.user_management_service.UpdateUserRequest
import com.google.protobuf.field_mask.FieldMask
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

import java.util.UUID
import scala.concurrent.Future

final class UpdateUserAuthIT extends AdminOrIDPAdminServiceCallAuthTests with UserManagementAuth {

  override def serviceCallName: String = "UserManagementService#UpdateUser"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    for {
      response <- createFreshUser(context.token, context.identityProviderId)
      _ <- stub(context.token).updateUser(
        UpdateUserRequest(
          user = response.user,
          updateMask = Some(FieldMask(scala.Seq("is_deactivated"))),
        )
      )
    } yield ()

  it should "deny calls if user is created already within another IDP" taggedAs adminSecurityAsset
    .setAttack(
      attackPermissionDenied(threat = "Present an existing userId but foreign Identity Provider")
    ) in {
    expectPermissionDenied {
      val userId = "fresh-user-" + UUID.randomUUID().toString
      for {
        response1 <- createConfig(canReadAsAdminStandardJWT)
        response2 <- createConfig(canReadAsAdminStandardJWT)

        response <- createFreshUser(
          userId,
          canReadAsAdmin.token,
          identityProviderId(response1),
          Seq.empty,
        )

        _ <- stub(canReadAsAdmin.token).updateUser(
          UpdateUserRequest(
            user = response.user.map(user =>
              user.copy(identityProviderId = identityProviderId(response2))
            ),
            updateMask = Some(FieldMask(scala.Seq("is_deactivated"))),
          )
        )

      } yield ()
    }
  }

}
