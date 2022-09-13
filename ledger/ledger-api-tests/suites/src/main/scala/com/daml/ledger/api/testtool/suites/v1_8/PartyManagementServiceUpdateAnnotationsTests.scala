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
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta

trait PartyManagementServiceUpdateAnnotationsTests { self: PartyManagementServiceIT =>

  test(
    "PMUpdateAnnotationsWithNoUpdateModifier",
    "Updating annotations using update paths with no update modifiers",
    enabled = features => features.userAndPartyManagementExtensionsForHub,
    partyAllocation = allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    implicit val l: ParticipantTestContext = ledger
    for {
      // update 1 - with a non-empty map and the exact update path (resulting in a successful merge update)
      _ <- withFreshParty(annotations = Map("k1" -> "v1", "k2" -> "v2")) { partyDetails =>
        ledger
          .updatePartyDetails(
            updateRequest(
              party = partyDetails.party,
              annotations = Map("k1" -> "v1a", "k3" -> "v3"),
              updatePaths = Seq("party_details.local_metadata.annotations"),
            )
          )
          .map { updateResp =>
            assertEquals(
              "updated annotations 1 ",
              extractUpdatedAnnotations(updateResp),
              Map("k1" -> "v1a", "k2" -> "v2", "k3" -> "v3"),
            )
          }
      }
      // update 2 - with a non-empty map and a shorthand update path (resulting in a successful merge update)
      _ <- withFreshParty(annotations = Map("k1" -> "v1", "k2" -> "v2")) { partyDetails =>
        ledger
          .updatePartyDetails(
            updateRequest(
              party = partyDetails.party,
              annotations = Map("k1" -> "v1b", "k3" -> "v3"),
              updatePaths = Seq("party_details"),
            )
          )
          .map { updateResp =>
            assertEquals(
              "updated annotations 2",
              extractUpdatedAnnotations(updateResp),
              Map("k1" -> "v1b", "k2" -> "v2", "k3" -> "v3"),
            )
          }
      }
      // update 3 - with the empty map and the the exact update path (resulting in a successful replace update)
      _ <- withFreshParty(annotations = Map("k1" -> "v1", "k2" -> "v2")) { partyDetails =>
        ledger
          .updatePartyDetails(
            updateRequest(
              party = partyDetails.party,
              annotations = Map.empty,
              updatePaths = Seq("party_details.local_metadata.annotations"),
            )
          )
          .map { updateResp =>
            assertEquals(
              "updated annotations 3",
              extractUpdatedAnnotations(updateResp),
              Map.empty,
            )
          }
      }
      // update 4 - with the empty map and a shorthand update path (resulting in a failed update as the update request is a no-op update)
      _ <- withFreshParty(annotations = Map("k1" -> "v1", "k2" -> "v2")) { partyDetails =>
        ledger
          .updatePartyDetails(
            updateRequest(
              party = partyDetails.party,
              annotations = Map.empty,
              updatePaths = Seq("party_details"),
            )
          )
          .mustFailWith(
            "empty update fail",
            errorCode = LedgerApiErrors.Admin.PartyManagement.InvalidUpdatePartyDetailsRequest,
            exceptionMessageSubstring = Some(
              s"INVALID_ARGUMENT: INVALID_PARTY_DETAILS_UPDATE(8,0): Update operation for party '${partyDetails.party}' failed due to: Update request describes a no-up update"
            ),
          )
      }
    } yield ()
  })

  test(
    "PMUpdateAnnotationsWithExplicitMergeUpdateModifier",
    "Updating annotations using update paths with explicit '!merge' update modifier",
    enabled = features => features.userAndPartyManagementExtensionsForHub,
    partyAllocation = allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      party <- ledger.allocateParty(localMetadata =
        Some(ObjectMeta(annotations = Map("k1" -> "v1", "k2" -> "v2")))
      )
      // update 1 - with a non-empty map and the exact update path (resulting in a successful merge update)
      update1 <- ledger.updatePartyDetails(
        updateRequest(
          party = party.toString,
          annotations = Map("k1" -> "v1a", "k3" -> "v3"),
          updatePaths = Seq("party_details.local_metadata.annotations!merge"),
        )
      )
      _ = assertEquals(
        "updated annotations 1 ",
        extractUpdatedAnnotations(update1),
        Map("k1" -> "v1a", "k2" -> "v2", "k3" -> "v3"),
      )
      // update 2 - with a non-empty map and a shorthand update path (resulting in a successful merge update)
      update2 <- ledger.updatePartyDetails(
        updateRequest(
          party = party.toString,
          annotations = Map("k1" -> "v1b", "k4" -> "v4"),
          updatePaths = Seq("party_details!merge"),
        )
      )
      _ = assertEquals(
        "updated annotations 2",
        extractUpdatedAnnotations(update2),
        Map("k1" -> "v1b", "k2" -> "v2", "k3" -> "v3", "k4" -> "v4"),
      )
      // update 3 - with the empty map and the exact update path (resulting in a failed update request
      //            as explicit merge on an exact field path with a default value is invalid)
      update3 <- ledger
        .updatePartyDetails(
          updateRequest(
            party = party.toString,
            annotations = Map.empty,
            updatePaths = Seq("party_details.local_metadata.annotations!merge"),
          )
        )
        .mustFail("explicit merge update on the exact annotations field path")
      _ = assertGrpcError(
        update3,
        errorCode = LedgerApiErrors.Admin.PartyManagement.InvalidUpdatePartyDetailsRequest,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_PARTY_DETAILS_UPDATE(8,0): Update operation for party '${party}' failed due to: MergeUpdateModifierOnEmptyMapField"
        ),
      )
      // update 4 - with the empty map and a shorthand update path (resulting in a failed update as the update request is a no-op update)
      update4 <- ledger
        .updatePartyDetails(
          updateRequest(
            party = party.toString,
            annotations = Map.empty,
            updatePaths = Seq("party_details!merge"),
          )
        )
        .mustFail("empty update fail")
      _ = assertGrpcError(
        update4,
        errorCode = LedgerApiErrors.Admin.PartyManagement.InvalidUpdatePartyDetailsRequest,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_PARTY_DETAILS_UPDATE(8,0): Update operation for party '${party}' failed due to: Update request describes a no-up update"
        ),
      )
    } yield ()
  })

  test(
    "PMUpdateAnnotationsWithExplicitReplaceUpdateModifier",
    "Updating annotations using update paths with explicit '!replace' update modifier",
    enabled = features => features.userAndPartyManagementExtensionsForHub,
    partyAllocation = allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      party <- ledger.allocateParty(localMetadata =
        Some(ObjectMeta(annotations = Map("k1" -> "v1", "k2" -> "v2")))
      )
      // update 1 - with a non-empty map and the exact update path (resulting in a successful replace update)
      update1 <- ledger.updatePartyDetails(
        updateRequest(
          party = party.toString,
          annotations = Map("k1" -> "v1a", "k3" -> "v3"),
          updatePaths = Seq("party_details.local_metadata.annotations!replace"),
        )
      )
      _ = assertEquals(
        "updated annotations 1",
        extractUpdatedAnnotations(update1),
        Map("k1" -> "v1a", "k3" -> "v3"),
      )
      // update 2 - with a non-empty map and a shorthand update path (resulting in a successful replace update)
      update2 <- ledger.updatePartyDetails(
        updateRequest(
          party = party.toString,
          annotations = Map("k1" -> "v1b", "k4" -> "v4"),
          updatePaths = Seq("party_details!replace"),
        )
      )
      _ = assertEquals(
        "updated annotations 2",
        extractUpdatedAnnotations(update2),
        Map("k1" -> "v1b", "k4" -> "v4"),
      )
      // update 3 - with the empty map and the the exact update path (resulting in a successful replace update)
      update3 <- ledger.updatePartyDetails(
        updateRequest(
          party = party.toString,
          annotations = Map.empty,
          updatePaths = Seq("party_details.local_metadata.annotations!replace"),
        )
      )
      _ = assertEquals(
        "updated annotations 3",
        extractUpdatedAnnotations(update3),
        Map.empty,
      )
      // update 4 - with the empty map and a shorthand update path (resulting in a successful replace update)
      update4 <- ledger.updatePartyDetails(
        updateRequest(
          party = party.toString,
          annotations = Map.empty,
          updatePaths = Seq("party_details!replace"),
        )
      )
      _ = assertEquals(
        "updated annotations 4",
        extractUpdatedAnnotations(update4),
        Map.empty,
      )
    } yield ()
  })

}
