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
    "Update all updatable fields",
    enabled = features => features.userAndPartyLocalMetadataExtensions,
    partyAllocation = allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    implicit val l: ParticipantTestContext = ledger
    for {
      _ <- withFreshParty(
        annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3")
      ) { partyDetails =>
        ledger
          .updatePartyDetails(
            updateRequest(
              party = partyDetails.party,
              annotations = Map("k1" -> "v1a", "k3" -> "", "k4" -> "v4"),
              updatePaths = Seq(
                "local_metadata.annotations"
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
                      Some(ObjectMeta(annotations = Map("k1" -> "v1a", "k2" -> "v2", "k4" -> "v4"))),
                  )
                )
              ),
            )
          }
      }
    } yield ()
  })

  test(
    "PMFailAttemptingToUpdateIsLocal",
    "Fail attempting to update is_local attribute",
    enabled = features => features.userAndPartyLocalMetadataExtensions,
    partyAllocation = allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    implicit val l: ParticipantTestContext = ledger
    for {
      _ <- withFreshParty(
      ) { partyDetails =>
        ledger
          .updatePartyDetails(
            updateRequest(
              party = partyDetails.party,
              isLocal = !partyDetails.isLocal,
              updatePaths = Seq(
                "is_local"
              ),
            )
          )
          .mustFailWith(
            "bad annotations key syntax on a user update",
            errorCode = LedgerApiErrors.Admin.PartyManagement.InvalidUpdatePartyDetailsRequest,
            exceptionMessageSubstring = Some(
              s"INVALID_ARGUMENT: INVALID_PARTY_DETAILS_UPDATE_REQUEST(8,0): Update operation for party '${partyDetails.party}' failed due to: Update request attempted to modify not-modifiable 'is_local' attribute"
            ),
          )
      }
    } yield ()
  })

  test(
    "PMFailAttemptingToUpdateDisplayName",
    "Fail attempting to update display_name attribute",
    enabled = features => features.userAndPartyLocalMetadataExtensions,
    partyAllocation = allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    implicit val l: ParticipantTestContext = ledger
    for {
      _ <- withFreshParty(
      ) { partyDetails =>
        ledger
          .updatePartyDetails(
            updateRequest(
              party = partyDetails.party,
              displayName = partyDetails.displayName + "different",
              updatePaths = Seq(
                "display_name"
              ),
            )
          )
          .mustFailWith(
            "bad annotations key syntax on a user update",
            errorCode = LedgerApiErrors.Admin.PartyManagement.InvalidUpdatePartyDetailsRequest,
            exceptionMessageSubstring = Some(
              s"INVALID_ARGUMENT: INVALID_PARTY_DETAILS_UPDATE_REQUEST(8,0): Update operation for party '${partyDetails.party}' failed due to: Update request attempted to modify not-modifiable 'display_name' attribute"
            ),
          )
      }
    } yield ()
  })

  test(
    "PMAllowSpecifyingIsLocalAndDisplayNameIfMatchingTheRealValues",
    "Allow specifying is_local and display_name if values in the update request match real values",
    enabled = features => features.userAndPartyLocalMetadataExtensions,
    partyAllocation = allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    implicit val l: ParticipantTestContext = ledger
    for {
      _ <- withFreshParty(
      ) { partyDetails =>
        ledger
          .updatePartyDetails(
            updateRequest(
              party = partyDetails.party,
              isLocal = partyDetails.isLocal,
              displayName = partyDetails.displayName,
              updatePaths = Seq(
                "display_name",
                "is_local",
              ),
            )
          )
          .map { updateResp =>
            assertEquals(
              "updating user",
              unsetResourceVersion(updateResp),
              unsetResourceVersion(UpdatePartyDetailsResponse(Some(partyDetails))),
            )
          }
      }
    } yield ()
  })

}
