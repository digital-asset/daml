// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  NoParties,
  Participant,
  Participants,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext

trait PartyManagementServiceUpdateAnnotationsTests { self: PartyManagementServiceIT =>

  test(
    "PMUpdateAnnotations",
    "Updating annotations using update paths",
    enabled = features => features.userAndPartyLocalMetadataExtensions,
    partyAllocation = allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    implicit val l: ParticipantTestContext = ledger
    for {
      // update 1 - with a non-empty map and the exact update path
      _ <- withFreshParty(annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3")) {
        partyDetails =>
          ledger
            .updatePartyDetails(
              updateRequest(
                party = partyDetails.party,
                annotations = Map("k1" -> "v1a", "k3" -> "", "k4" -> "v4"),
                updatePaths = Seq("local_metadata.annotations"),
              )
            )
            .map { updateResp =>
              assertEquals(
                "updated annotations 1",
                extractUpdatedAnnotations(updateResp),
                Map("k1" -> "v1a", "k2" -> "v2", "k4" -> "v4"),
              )
            }
      }
      // update 2 - with a non-empty map and a shorthand update path
      _ <- withFreshParty(annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3")) {
        partyDetails =>
          ledger
            .updatePartyDetails(
              updateRequest(
                party = partyDetails.party,
                annotations = Map("k1" -> "v1a", "k3" -> "", "k4" -> "v4"),
                updatePaths = Seq("local_metadata"),
              )
            )
            .map { updateResp =>
              assertEquals(
                "updated annotations 2",
                extractUpdatedAnnotations(updateResp),
                Map("k1" -> "v1a", "k2" -> "v2", "k4" -> "v4"),
              )
            }
      }
      // update 3 - with the empty map and the the exact update path
      _ <- withFreshParty(annotations = Map("k1" -> "v1", "k2" -> "v2")) { partyDetails =>
        ledger
          .updatePartyDetails(
            updateRequest(
              party = partyDetails.party,
              annotations = Map.empty,
              updatePaths = Seq("local_metadata.annotations"),
            )
          )
          .map { updateResp =>
            assertEquals(
              "updated annotations 3",
              extractUpdatedAnnotations(updateResp),
              Map("k1" -> "v1", "k2" -> "v2"),
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
              updatePaths = Seq("local_metadata"),
            )
          )
          .map { updateResp =>
            assertEquals(
              "updated annotations 3",
              extractUpdatedAnnotations(updateResp),
              Map("k1" -> "v1", "k2" -> "v2"),
            )
          }
      }
    } yield ()
  })

}
