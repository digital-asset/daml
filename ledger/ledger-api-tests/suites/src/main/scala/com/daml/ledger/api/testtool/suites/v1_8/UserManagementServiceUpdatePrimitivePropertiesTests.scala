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
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext

// Test updating a primitive fields taking primaryParty as a representative example of such a field.
trait UserManagementServiceUpdatePrimitivePropertiesTests {
  self: UserManagementServiceIT =>

  test(
    "UpdatePrimaryPartyWithNoUpdateModifier",
    "Update primary party using update paths with no update modifiers",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    implicit val l: ParticipantTestContext = ledger
    for {
      _ <- withFreshUser(primaryParty = "primaryParty0") { user =>
        // update 1 - with a non-empty primary party and the exact update path (resulting in a successful update)
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
      }
      // update 2 - with a non-empty primary party and a shorthand update path (resulting in a successful update)
      _ <- withFreshUser(primaryParty = "primaryParty0") { user =>
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
      }
      // update 3 - with the empty primary party and the the exact update path (resulting in a successful update)
      _ <- withFreshUser(primaryParty = "primaryParty0") { user =>
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
      }
      // update 4 - with the empty primary party and a shorthand update path (resulting in a failed update as the update request is a no-op update)
      _ <- withFreshUser(primaryParty = "primaryParty0") { user =>
        ledger.userManagement
          .updateUser(updateRequest(id = user.id, primaryParty = "", updatePaths = Seq("user")))
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
    "UpdatePrimaryPartyWithExplicitMergeUpdateModifier",
    "Update primary using update paths with explicit '!merge' update modifier",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    implicit val l: ParticipantTestContext = ledger
    for {
      // update 1 - with a non-empty primary party and the exact update path (resulting in a failed update
      //            as the explicit merge modifier on an exact field paths is invalid)
      _ <- withFreshUser(primaryParty = "primaryParty0") { user =>
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
      }
      // update 2 - with a non-empty map and a shorthand update path (resulting in a successful update)
      _ <- withFreshUser(primaryParty = "primaryParty0") { user =>
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
      }
      // update 3 - with the empty map and the exact update path (resulting in a failed update
      //            as the explicit merge modifier on an exact field paths is invalid)
      _ <- withFreshUser(primaryParty = "primaryParty0") { user =>
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
            errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUserUpdate,
            exceptionMessageSubstring = Some(
              s"INVALID_ARGUMENT: INVALID_USER_UPDATE(8,0): Update operation for user id '${user.id}' failed due to: MergeUpdateModifierOnPrimitiveFieldDefaultValueUpdate"
            ),
          )
      }
      // update 4 - with the empty map and a shorthand update path (resulting in a failed update as the update request is a no-op update)
      _ <- withFreshUser(primaryParty = "primaryParty0") { user =>
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
            errorCode = LedgerApiErrors.Admin.UserManagement.InvalidUserUpdate,
            exceptionMessageSubstring = Some(
              s"INVALID_ARGUMENT: INVALID_USER_UPDATE(8,0): Update operation for user id '${user.id}' failed due to: Update request describes a no-up update"
            ),
          )
      }
    } yield ()
  })

  test(
    "UpdatePrimaryPartyWithExplicitReplaceUpdateModifier",
    "Update primary party using update paths with explicit '!replace' update modifier",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    implicit val l: ParticipantTestContext = ledger
    for {
      // update 1 - with a non-empty primary party and the exact update path (resulting in a successful update)
      _ <- withFreshUser(primaryParty = "primaryParty0") { user =>
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
      }
      // update 2 - with a non-empty primary party and a shorthand update path (resulting in a successful update)
      _ <- withFreshUser(primaryParty = "primaryParty0") { user =>
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
      }
      // update 3 - with the empty primary party and the the exact update path (resulting in a successful update)
      _ <- withFreshUser(primaryParty = "primaryParty0") { user =>
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
      }
      // update 4 - with the empty primary party and a shorthand update path (resulting in a successful replace update)
      _ <- withFreshUser(primaryParty = "primaryParty0") { user =>
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
      }
    } yield ()
  })

}
