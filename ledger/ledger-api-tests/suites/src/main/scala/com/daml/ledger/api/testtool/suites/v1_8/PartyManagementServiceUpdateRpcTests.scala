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
import com.daml.ledger.api.testtool.infrastructure.Assertions.{assertEquals, assertGrpcError}
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.party_management_service.{
  PartyDetails,
  UpdatePartyDetailsRequest,
  UpdatePartyDetailsResponse,
}
import com.daml.lf.data.Ref
import com.google.protobuf.field_mask.FieldMask
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext

trait PartyManagementServiceUpdateRpcTests { self: PartyManagementServiceIT =>

  test(
    "UpdateAllUpdatableFields",
    "Update all updated fields",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    implicit val l: ParticipantTestContext = ledger
    for {
      _ <- withFreshParty(
        annotations = Map("k1" -> "v1", "k2" -> "v2")
      ) { partyDetails =>
        ledger
          .updatePartyDetails(
            updateRequest(
              party = partyDetails.party,
              annotations = Map("k1" -> "v1a", "k3" -> "v3"),
              updatePaths = Seq(
                "party_details.local_metadata.annotations!merge"
              ),
            )
          )
          .map { updateResp =>
            assertEquals(
              "updating user 1",
              unsetResourceVersion(updateResp),
              UpdatePartyDetailsResponse(
                Some(
                  PartyDetails(
                    party = partyDetails.party,
                    displayName = partyDetails.displayName,
                    isLocal = partyDetails.isLocal,
                    localMetadata =
                      Some(ObjectMeta(annotations = Map("k1" -> "v1a", "k2" -> "v2", "k3" -> "v3"))),
                  )
                )
              ),
            )
          }
      }
    } yield ()
  })

  test(
    "PMFailUpdatePartyWhenPartyDoesNotExist",
    "Failing when attempting a party which doesn't exist",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val party = Ref.Party.assertFromString("party1-non-existent")
    val partyDetails = PartyDetails(
      party = party,
      localMetadata = Some(
        ObjectMeta(resourceVersion = "", annotations = Map("k1" -> "v1"))
      ),
    )
    for {
      error <- ledger
        .updatePartyDetails(
          UpdatePartyDetailsRequest(
            partyDetails = Some(partyDetails),
            updateMask = Some(FieldMask(Seq("party_details.local_metadata.annotations"))),
          )
        )
        .mustFail("updating non-existent party")
      _ = assertGrpcError(
        error,
        errorCode = LedgerApiErrors.Admin.PartyManagement.PartyNotFound,
        exceptionMessageSubstring = Some(
          "NOT_FOUND: PARTY_NOT_FOUND(11,0): update participant party record failed for unknown party \"party1-non-existent\""
        ),
      )
    } yield ()
  })

  test(
    "PMInvalidUpdateRequests",
    "Failing update requests",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      party <- ledger.allocateParty(localMetadata =
        Some(ObjectMeta(annotations = Map("k1" -> "v1")))
      )
      // update 1 - unknown update modifier
      update1 <- ledger
        .updatePartyDetails(
          updateRequest(
            party = party.toString,
            annotations = Map("k2" -> "v2"),
            updatePaths = Seq("party_details.local_metadata.annotations!badmodifer"),
          )
        )
        .mustFail("update with an unknown update modifier")
      _ = assertGrpcError(
        update1,
        errorCode = LedgerApiErrors.Admin.PartyManagement.InvalidPartyDetailsUpdate,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_PARTY_DETAILS_UPDATE(8,0): Update operation for party '$party' failed due to: UnknownUpdateModifier"
        ),
      )
      // update 2 - unknown field path
      update2 <- ledger
        .updatePartyDetails(
          updateRequest(
            party = party.toString,
            annotations = Map("k2" -> "v2"),
            updatePaths = Seq("party_details.local_metadata.unknown_field"),
          )
        )
        .mustFail("update with an unknown update path")
      _ = assertGrpcError(
        update2,
        errorCode = LedgerApiErrors.Admin.PartyManagement.InvalidPartyDetailsUpdate,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_PARTY_DETAILS_UPDATE(8,0): Update operation for party '$party' failed due to: UnknownFieldPath"
        ),
      )
      // update 3 - bad update path syntax: unknown field path
      update3 <- ledger
        .updatePartyDetails(
          updateRequest(
            party = party.toString,
            annotations = Map("k2" -> "v2"),
            updatePaths = Seq("party_details.dummy"),
          )
        )
        .mustFail("update with an unknown update path")
      _ = assertGrpcError(
        update3,
        errorCode = LedgerApiErrors.Admin.PartyManagement.InvalidPartyDetailsUpdate,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_PARTY_DETAILS_UPDATE(8,0): Update operation for party '$party' failed due to: UnknownFieldPath"
        ),
      )
      // update 4 - bad update path syntax
      update1 <- ledger
        .updatePartyDetails(
          updateRequest(
            party = party.toString,
            annotations = Map("k2" -> "v2"),
            updatePaths = Seq("aaa!bad!qwerty"),
          )
        )
        .mustFail("update with an unknown update path")
      _ = assertGrpcError(
        update1,
        errorCode = LedgerApiErrors.Admin.PartyManagement.InvalidPartyDetailsUpdate,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_PARTY_DETAILS_UPDATE(8,0): Update operation for party '$party' failed due to: InvalidUpdatePathSyntax"
        ),
      )
      // update 5 - no update paths
      update1 <- ledger
        .updatePartyDetails(
          updateRequest(
            party = party.toString,
            annotations = Map("k2" -> "v2"),
            updatePaths = Seq.empty,
          )
        )
        .mustFail("update with an unknown update path")
      _ = assertGrpcError(
        update1,
        errorCode = LedgerApiErrors.Admin.PartyManagement.InvalidPartyDetailsUpdate,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_PARTY_DETAILS_UPDATE(8,0): Update operation for party '$party' failed due to: EmptyFieldMask"
        ),
      )
    } yield ()
  })

}
