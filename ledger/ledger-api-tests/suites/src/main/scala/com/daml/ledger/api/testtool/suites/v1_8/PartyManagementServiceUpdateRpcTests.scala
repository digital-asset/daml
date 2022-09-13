// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  NoParties,
  Participant,
  Participants,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.assertEquals
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.party_management_service.{
  PartyDetails,
  UpdatePartyDetailsResponse,
}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext

trait PartyManagementServiceUpdateRpcTests {
  self: PartyManagementServiceIT =>

  test(
    "PMUpdateAllUpdatableFields",
    "Update all updated fields",
    enabled = features => features.userAndPartyManagementExtensionsForHub,
    partyAllocation = allocate(NoParties),
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
}
