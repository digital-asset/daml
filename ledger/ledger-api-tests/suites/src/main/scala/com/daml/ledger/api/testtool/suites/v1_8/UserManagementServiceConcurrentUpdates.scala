// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Assertions.assertEquals
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.user_management_service.{
  CreateUserRequest,
  CreateUserResponse,
  GetUserRequest,
  UpdateUserRequest,
  UpdateUserResponse,
  User,
}
import com.google.protobuf.field_mask.FieldMask
import com.daml.ledger.api.testtool.infrastructure.Assertions.{_}

import scala.concurrent.Future
import scala.util.Success

trait UserManagementServiceConcurrentUpdates {
  self: UserManagementServiceIT =>

  userManagementTest(
    "TestUpdateUser",
    "Exercise UpdateUser rpc",
    requiresUserExtensionsForHub = true,
  )(implicit ec => { implicit ledger: ParticipantTestContext =>
    val userId1 = ledger.nextUserId()
    val user1a = User(
      id = userId1,
      primaryParty = "",
      isDeactivated = false,
      metadata = Some(ObjectMeta()),
    )
    for {
      res1: CreateUserResponse <- ledger.createUser(
        CreateUserRequest(Some(user1a), Nil)
      )
      _ = assertEquals(unsetResourceVersion(res1), CreateUserResponse(Some(user1a)))
      user1b = User(
        id = userId1,
        primaryParty = "primaryParty1",
        isDeactivated = true,
        metadata = Some(
          ObjectMeta(
            resourceVersion = "",
            annotations = Map(
              "key1" -> "val1",
              "key2" -> "val2",
            ),
          )
        ),
      )
      // Update with concurrent change detection disabled
      res2 <- ledger.userManagement.updateUser(
        UpdateUserRequest(
          user = Some(user1b),
          updateMask = Some(FieldMask(paths = Seq("user"))),
        )
      )
      _ = assertEquals(unsetResourceVersion(res2), UpdateUserResponse(Some(user1b)))
      _ = assertEquals(
        unsetResourceVersion(res2),
        UpdateUserResponse(Some(unsetResourceVersion(user1b))),
      )

      user1aResourceVersion = res1.user.get.metadata.get.resourceVersion
      _ = assertValidResourceVersionString(user1aResourceVersion, "newly created user")
      user1bResourceVersion = res2.user.get.metadata.get.resourceVersion
      _ = assertValidResourceVersionString(
        user1bResourceVersion,
        "updated user with concurrent change control disabled",
      )

      _ = assert(
        user1aResourceVersion != user1bResourceVersion,
        s"User's resource version before an update: ${user1aResourceVersion} must be different from other that after an update: ${user1bResourceVersion}",
      )

      user1c = user1b.update(_.metadata.resourceVersion := user1aResourceVersion)
      // Update with concurrent change detection enabled, but resourceVersion is outdated
      res3 <- ledger.userManagement
        .updateUser(
          UpdateUserRequest(
            user = Some(user1c),
            updateMask = Some(FieldMask(paths = Seq("user"))),
          )
        )
        .mustFail(
          "update a user using an out-of-date resource version for concurrent change detection"
        )
      _ = assertConcurrentUserUpdateDetectedError(res3)
      user1d = user1b.update(_.metadata.resourceVersion := user1bResourceVersion)
      // Update with concurrent change detection enabled and up-to-date resource version
      res4 <- ledger.userManagement.updateUser(
        UpdateUserRequest(
          user = Some(user1d),
          updateMask = Some(FieldMask(paths = Seq("user"))),
        )
      )
      _ = assertEquals(
        unsetResourceVersion(res4),
        UpdateUserResponse(Some(unsetResourceVersion(user1d))),
      )
      user1cResourceVersion = res4.user.get.metadata.get.resourceVersion
      _ = assert(
        user1bResourceVersion != user1cResourceVersion,
        s"User's resource versions before and after an update must be different but were the same: '$user1bResourceVersion'",
      )
      _ = assertValidResourceVersionString(
        user1bResourceVersion,
        "updated user with concurrent change control enabled",
      )
    } yield ()
  })

  userManagementTest(
    "RaceConditionUpdateUserAnnotations",
    "Tests scenario of multiple concurrent update annotations calls for the same user - merge semantics",
    runConcurrently = false,
    requiresUserExtensionsForHub = true,
  ) {
    implicit ec =>
      { participant =>
        val attempts = (1 to 10).toVector
        val userId = participant.nextUserId()
        for {
          _ <- participant.createUser(CreateUserRequest(Some(User(id = userId))))
          _ <- Future.traverse(attempts) { attemptNo =>
            participant.userManagement
              .updateUser(
                updateRequest(
                  id = userId,
                  annotations = Map(s"key$attemptNo" -> ""),
                  updatePaths = Seq("user"),
                )
              )
              .transform(Success(_))
          }
          get <- participant.userManagement.getUser(GetUserRequest(userId = userId))
        } yield {
          assertEquals(
            get.user.get.metadata.get.annotations,
            Map(
              "key1" -> "",
              "key2" -> "",
              "key3" -> "",
              "key4" -> "",
              "key5" -> "",
              "key6" -> "",
              "key7" -> "",
              "key8" -> "",
              "key9" -> "",
              "key10" -> "",
            ),
          )
        }
      }
  }

}
