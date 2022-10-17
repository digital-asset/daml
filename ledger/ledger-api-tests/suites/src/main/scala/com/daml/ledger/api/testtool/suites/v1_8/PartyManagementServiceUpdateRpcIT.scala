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
  UpdatePartyDetailsRequest,
  UpdatePartyDetailsResponse,
}
import com.google.protobuf.field_mask.FieldMask

class PartyManagementServiceUpdateRpcIT extends PartyManagementITBase {

  testWithFreshPartyDetails(
    "PMUpdatingEmptyDisplayName",
    "Attempting to unset displayName for party which doesn't have a displayName should be a successful no-op update",
    requiresUserAndPartyLocalMetadataExtensions = true,
  )(displayName = "")(implicit ec =>
    implicit ledger =>
      partyDetails =>
        ledger
          .updatePartyDetails(
            updateRequest(
              party = partyDetails.party,
              displayName = "",
              updatePaths = Seq(
                "display_name"
              ),
            )
          )
          .map { updateResp =>
            assertEquals(
              "updating user 1",
              unsetResourceVersion(updateResp),
              UpdatePartyDetailsResponse(Some(unsetResourceVersion(partyDetails))),
            )
          }
  )

  testWithFreshPartyDetails(
    "PMUpdateAllUpdatableFields",
    "Update all updatable fields",
    requiresUserAndPartyLocalMetadataExtensions = true,
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
                    displayName = partyDetails.displayName,
                    isLocal = partyDetails.isLocal,
                    localMetadata =
                      Some(ObjectMeta(annotations = Map("k1" -> "v1a", "k2" -> "v2", "k4" -> "v4"))),
                  )
                )
              ),
            )
          }
  )

  testWithFreshPartyDetails(
    "PMFailAttemptingToUpdateIsLocal",
    "Fail attempting to update is_local attribute",
    requiresUserAndPartyLocalMetadataExtensions = true,
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
            errorCode = LedgerApiErrors.Admin.PartyManagement.InvalidUpdatePartyDetailsRequest,
            exceptionMessageSubstring = Some(
              s"INVALID_ARGUMENT: INVALID_PARTY_DETAILS_UPDATE_REQUEST(8,0): Update operation for party '${partyDetails.party}' failed due to: Update request attempted to modify not-modifiable 'is_local' attribute"
            ),
          )
  )

  testWithFreshPartyDetails(
    "PMFailAttemptingToUpdateDisplayName",
    "Fail attempting to update display_name attribute",
    requiresUserAndPartyLocalMetadataExtensions = true,
  )()(implicit ec =>
    implicit ledger =>
      partyDetails =>
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
  )

  testWithFreshPartyDetails(
    "PMAllowSpecifyingIsLocalAndDisplayNameIfMatchingTheRealValues",
    "Allow specifying is_local and display_name if values in the update request match real values",
    requiresUserAndPartyLocalMetadataExtensions = true,
  )(displayName = "displayName1")(implicit ec =>
    implicit ledger =>
      partyDetails =>
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
  )

  testWithFreshPartyDetails(
    "UpdatePartyDetailsEvenIfMetadataIsNotSetInUpdateRequest",
    "Update a party details even if the metadata field is not set in the update request",
    requiresUserAndPartyLocalMetadataExtensions = true,
  )()(implicit ec =>
    implicit ledger =>
      partyDetails =>
        ledger
          .updatePartyDetails(
            UpdatePartyDetailsRequest(
              partyDetails = Some(
                PartyDetails(
                  party = partyDetails.party,
                  localMetadata = None,
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
    enabled = _.userAndPartyLocalMetadataExtensions,
  )(implicit ec => { case Participants(Participant(ledger)) =>
    ledger
      .updatePartyDetails(
        UpdatePartyDetailsRequest(
          partyDetails = None,
          updateMask = Some(FieldMask(Seq("local_metadata"))),
        )
      )
      .mustFailWith(
        "update with an unknown update path",
        errorCode = LedgerApiErrors.RequestValidation.MissingField,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: MISSING_FIELD(8,0): The submitted command is missing a mandatory field: party_details"
        ),
      )
  })

  test(
    "FailUpdateNonExistentParty",
    "Fail when attempting to update a non-existent party",
    allocate(NoParties),
    enabled = _.userAndPartyLocalMetadataExtensions,
  )(implicit ec => { case Participants(Participant(ledger)) =>
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
          errorCode = LedgerApiErrors.Admin.PartyManagement.PartyNotFound,
          exceptionMessageSubstring = Some(
            s"NOT_FOUND: PARTY_NOT_FOUND(11,0): Party: '$party' was not found when updating a party record"
          ),
        )
    } yield ()
  })
}
