// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.nio.charset.StandardCharsets

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.v1.admin.user_management_service.{CreateUserRequest, CreateUserResponse}
import com.daml.ledger.api.testtool.infrastructure.Assertions.{assertEquals, _}

// TODO um-for-hub: Consider making these common for user and parites
trait UserManagementServiceAnnotationsValidationTests { self: UserManagementServiceIT =>

  private val maxAnnotationsSizeInBytes = 256 * 1024

  userManagementTest(
    "TestAnnotationsSizeLimits",
    "Test annotations' size limit",
    requiresUserAndPartyLocalMetadataExtensions = true,
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val largeString = "a" * maxAnnotationsSizeInBytes
    val notSoLargeString = "a" * (maxAnnotationsSizeInBytes - 1)
    assertEquals(largeString.getBytes(StandardCharsets.UTF_8).length, maxAnnotationsSizeInBytes)
    val user1 = newUser(id = userId1, annotations = Map("a" -> largeString))
    val user2 = newUser(id = userId1, annotations = Map("a" -> notSoLargeString))
    for {
      _ <- ledger
        .createUser(CreateUserRequest(Some(user1)))
        .mustFailWith(
          "total size of annotations exceeds 256kb max limit",
          errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
          exceptionMessageSubstring = Some(
            "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: annotations from field 'user.metadata.annotations' are larger than the limit of 256kb"
          ),
        )
      _ <- ledger.createUser(CreateUserRequest(Some(user2)))
      _ <- ledger.userManagement
        .updateUser(
          updateRequest(
            id = userId1,
            annotations = Map("a" -> largeString),
            updatePaths = Seq("metadata.annotations"),
          )
        )
        .mustFailWith(
          "total size of annotations, in a user update call, is over 256kb",
          errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
          exceptionMessageSubstring = Some(
            "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: annotations from field 'user.metadata.annotations' are larger than the limit of 256kb"
          ),
        )
    } yield ()
  })

  userManagementTestWithFreshUser(
    "NotCountingRemovedKeysToTheSizeLimit",
    "Do not count the keys that are to be deleted towards the annotations size limit",
  )() { implicit ec => (ledger, user) =>
    val largeString = "b" * (maxAnnotationsSizeInBytes - 1)
    val anno1 = Map("a" -> largeString, "c" -> "d")
    val anno2 = Map("a" -> largeString, "cc" -> "")
    def getAnnotationsBytes(anno: Map[String, String]): Int = anno.iterator.map { case (k, v) =>
      k.getBytes(StandardCharsets.UTF_8).length + v.getBytes(StandardCharsets.UTF_8).length
    }.sum
    assertEquals(
      "comparing annotation sizes",
      getAnnotationsBytes(anno1),
      getAnnotationsBytes(anno2),
    )
    for {
      _ <- ledger.userManagement
        .updateUser(
          updateRequest(
            id = user.id,
            annotations = anno1,
            updatePaths = Seq("metadata"),
          )
        )
        .mustFailWith(
          "updating and exceeding annotations limit",
          LedgerApiErrors.RequestValidation.InvalidArgument,
          Some(
            "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: annotations from field 'user.metadata.annotations' are larger than the limit of 256kb"
          ),
        )

      _ <- ledger.userManagement
        .updateUser(
          updateRequest(
            id = user.id,
            annotations = anno2,
            updatePaths = Seq("metadata"),
          )
        )
        .map { updateResp =>
          assertEquals(
            "updating and not exceeding annotations limit because deletions are not counted towards the limit",
            extractUpdatedAnnotations(updateResp),
            Map("a" -> largeString),
          )
        }
    } yield ()

  }

  userManagementTest(
    "TestAnnotationsKeySyntax",
    "Test annotations' key syntax",
    requiresUserAndPartyLocalMetadataExtensions = true,
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val invalidKey = ".user.management.daml/foo_"
    val user1 = newUser(id = userId1, annotations = Map("0-user.management.daml/foo" -> "a"))
    for {
      create1 <- ledger.createUser(CreateUserRequest(Some(user1)))
      _ = assertEquals(
        unsetResourceVersion(create1),
        CreateUserResponse(Some(user1)),
      )
      _ <- ledger.userManagement
        .updateUser(
          updateRequest(
            id = userId1,
            annotations = Map(invalidKey -> "a"),
            updatePaths = Seq("metadata.annotations"),
          )
        )
        .mustFailWith(
          "bad annotations key syntax on a user update",
          errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
          exceptionMessageSubstring = Some(
            "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Key prefix segment '.user.management.daml' has invalid syntax"
          ),
        )
      _ <- ledger
        .createUser(
          CreateUserRequest(Some(newUser(id = userId2, annotations = Map(invalidKey -> "a"))))
        )
        .mustFailWith(
          "bad annotations key syntax on user creation",
          errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
          exceptionMessageSubstring = Some(
            "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Key prefix segment '.user.management.daml' has invalid syntax"
          ),
        )
    } yield ()
  })

}
