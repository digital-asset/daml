// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  NoParties,
  Participant,
  Participants,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.assertEquals
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext

trait UserManagementServiceUpdateAnnotationMapTests {
  self: UserManagementServiceUpdateRpcTests with UserManagementServiceIT =>
  test(
    "UpdateAnnotationsWithNoUpdateModifier",
    "Update annotations using update paths with no update modifiers",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    implicit val l: ParticipantTestContext = ledger
    for {
      // update 1 - with a non-empty map and the exact update path (resulting in a successful merge update)
      _ <- withFreshUser(annotations = Map("k1" -> "v1", "k2" -> "v2")) { user =>
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
      }
      // update 2 - with a non-empty map and a shorthand update path (resulting in a successful merge update)
      _ <- withFreshUser(annotations = Map("k1" -> "v1", "k2" -> "v2")) { user =>
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
      }
      // update 3 - with the empty map and the the exact update path (resulting in a successful replace update)
      _ <- withFreshUser(annotations = Map("k1" -> "v1", "k2" -> "v2")) { user =>
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
      }
      // update 4 - with the empty map and a shorthand update path (resulting in a failed update as the update request is a no-op update)
      _ <- withFreshUser(annotations = Map("k1" -> "v1", "k2" -> "v2")) { user =>
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
            errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUserUpdate,
            exceptionMessageSubstring = Some(
              s"INVALID_ARGUMENT: INVALID_USER_UPDATE(8,0): Update operation for user id '${user.id}' failed due to: Update request describes a no-up update"
            ),
          )
      }
    } yield ()
  })

  test(
    "UpdateAnnotationsWithExplicitMergeUpdateModifier",
    "Update annotations using update paths with explicit '!merge' update modifier",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    implicit val l: ParticipantTestContext = ledger
    for {
      // update 1 - with a non-empty map and the exact update path (resulting in a successful merge update)
      _ <- withFreshUser(annotations = Map("k1" -> "v1", "k2" -> "v2")) { user =>
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
      }
      // update 2 - with a non-empty map and a shorthand update path (resulting in a successful merge update)
      _ <- withFreshUser(annotations = Map("k1" -> "v1", "k2" -> "v2")) { user =>
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
      }
      // update 3 - with the empty map and the exact update path (resulting in a failed update request
      //            as explicit merge on an exact field path with a default value is invalid)
      _ <- withFreshUser(annotations = Map("k1" -> "v1", "k2" -> "v2")) { user =>
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
            errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUserUpdate,
            exceptionMessageSubstring = Some(
              s"INVALID_ARGUMENT: INVALID_USER_UPDATE(8,0): Update operation for user id '${user.id}' failed due to: MergeUpdateModifierOnEmptyMapField"
            ),
          )
      }
      // update 4 - with the empty map and a shorthand update path (resulting in a failed update as the update request is a no-op update)
      _ <- withFreshUser(annotations = Map("k1" -> "v1", "k2" -> "v2")) { user =>
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
            errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUserUpdate,
            exceptionMessageSubstring = Some(
              s"INVALID_ARGUMENT: INVALID_USER_UPDATE(8,0): Update operation for user id '${user.id}' failed due to: Update request describes a no-up update"
            ),
          )
      }
    } yield ()
  })

  test(
    "UpdateAnnotationsWithExplicitReplaceUpdateModifier",
    "Update annotations using update paths with explicit '!replace' update modifier",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    implicit val l: ParticipantTestContext = ledger
    for {
      // update 1 - with a non-empty map and the exact update path (resulting in a successful replace update)
      _ <- withFreshUser(annotations = Map("k1" -> "v1", "k2" -> "v2")) { user =>
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
      }
      // update 2 - with a non-empty map and a shorthand update path (resulting in a successful replace update)
      _ <- withFreshUser(annotations = Map("k1" -> "v1", "k2" -> "v2")) { user =>
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
      }
      // update 3 - with the empty map and the the exact update path (resulting in a successful replace update)
      _ <- withFreshUser(annotations = Map("k1" -> "v1", "k2" -> "v2")) { user =>
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
      }
      // update 4 - with the empty map and a shorthand update path (resulting in a successful replace update)
      _ <- withFreshUser(annotations = Map("k1" -> "v1", "k2" -> "v2")) { user =>
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
      }
    } yield ()
  })

}
