// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.{
  NoParties,
  Participant,
  Participants,
  allocate,
}
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.TestConstraints
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.party_management_service.{
  PartyDetails,
  UpdatePartyDetailsRequest,
  UpdatePartyDetailsResponse,
}
import com.digitalasset.canton.ledger.error.groups.{AdminServiceErrors, RequestValidationErrors}
import com.google.protobuf.field_mask.FieldMask

class PartyManagementServiceUpdateRpcIT extends PartyManagementITBase {

  testWithFreshPartyDetails(
    "PMUpdateAllUpdatableFields",
    "Update all updatable fields",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"))(implicit ec =>
    implicit ledger =>
      partyDetails =>
        ledger
          .updatePartyDetails(
            updateRequest(
              party = partyDetails.party,
              annotations = Map("k1" -> "v1a", "k3" -> "", "k4" -> "v4", "k5" -> ""),
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
                    isLocal = partyDetails.isLocal,
                    localMetadata = Some(
                      ObjectMeta(
                        resourceVersion = "",
                        annotations = Map("k1" -> "v1a", "k2" -> "v2", "k4" -> "v4"),
                      )
                    ),
                    identityProviderId = "",
                  )
                )
              ),
            )
          }
  )

  testWithFreshPartyDetails(
    "PMFailAttemptingToUpdateIsLocal",
    "Fail attempting to update is_local attribute",
  )()(implicit ec =>
    implicit ledger =>
      partyDetails =>
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
            errorCode = AdminServiceErrors.PartyManagement.InvalidUpdatePartyDetailsRequest,
          )
  )

  testWithFreshPartyDetails(
    "PMAllowSpecifyingIsLocalAndDisplayNameIfMatchingTheRealValues",
    "Allow specifying is_local if values in the update request match real values",
  )()(implicit ec =>
    implicit ledger =>
      partyDetails =>
        ledger
          .updatePartyDetails(
            updateRequest(
              party = partyDetails.party,
              isLocal = partyDetails.isLocal,
              updatePaths = Seq(
                "is_local"
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
  )

  testWithFreshPartyDetails(
    "UpdatePartyDetailsEvenIfMetadataIsNotSetInUpdateRequest",
    "Update a party details even if the metadata field is not set in the update request",
  )()(implicit ec =>
    implicit ledger =>
      partyDetails =>
        ledger
          .updatePartyDetails(
            UpdatePartyDetailsRequest(
              partyDetails = Some(
                PartyDetails(
                  party = partyDetails.party,
                  isLocal = false,
                  localMetadata = None,
                  identityProviderId = "",
                )
              ),
              updateMask = Some(FieldMask(Seq("party"))),
            )
          )
          .map { updateResp =>
            assertEquals(
              "update with the metadata not set in the request",
              unsetResourceVersion(updateResp),
              UpdatePartyDetailsResponse(Some(newPartyDetails(partyDetails.party))),
            )
          }
  )

  test(
    "FailingUpdateRequestsWhenPartyDetailsFieldIsUnset",
    "Failing an update request when party_details field is unset",
    allocate(NoParties),
    limitation = TestConstraints.GrpcOnly(
      "JSON API returns a different error when party id is not set in update"
    ),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    ledger
      .updatePartyDetails(
        UpdatePartyDetailsRequest(
          partyDetails = None,
          updateMask = Some(FieldMask(Seq("local_metadata"))),
        )
      )
      .mustFailWith(
        "update with an unknown update path",
        errorCode = RequestValidationErrors.MissingField,
      )
  })

  test(
    "FailUpdateNonExistentParty",
    "Fail when attempting to update a non-existent party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val party = ledger.nextPartyId()
    for {
      _ <- ledger
        .updatePartyDetails(
          updateRequest(
            party = party,
            annotations = Map("k1" -> "v1"),
            updatePaths = Seq("local_metadata.annotations"),
          )
        )
        .mustFailWith(
          "updating a non-existent party",
          errorCode = AdminServiceErrors.PartyManagement.PartyNotFound,
        )
    } yield ()
  })
}
