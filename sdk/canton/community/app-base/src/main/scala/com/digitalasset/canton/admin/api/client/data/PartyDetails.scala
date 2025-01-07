// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta as ProtoObjectMeta
import com.daml.ledger.api.v2.admin.party_management_service.PartyDetails as ProtoPartyDetails
import com.digitalasset.canton.topology.PartyId

import scala.util.control.NoStackTrace

/** Represents a party details value exposed in the Canton console
  *
  * @param isLocal true if party is hosted by the participant and the party shares the same identity provider as the user issuing the request.
  */
final case class PartyDetails(
    party: PartyId,
    isLocal: Boolean,
    annotations: Map[String, String],
    identityProviderId: String,
)

object PartyDetails {
  def fromProtoPartyDetails(details: ProtoPartyDetails): PartyDetails = PartyDetails(
    party = PartyId.tryFromProtoPrimitive(details.party),
    isLocal = details.isLocal,
    annotations = details.localMetadata.fold(Map.empty[String, String])(_.annotations),
    identityProviderId = details.identityProviderId,
  )
  def toProtoPartyDetails(
      details: PartyDetails,
      resourceVersionO: Option[String],
  ): ProtoPartyDetails = ProtoPartyDetails(
    party = details.party.toString,
    isLocal = details.isLocal,
    localMetadata = Some(
      ProtoObjectMeta(
        resourceVersion = resourceVersionO.getOrElse(""),
        annotations = details.annotations,
      )
    ),
    identityProviderId = details.identityProviderId,
  )
}

final case class ModifyingNonModifiablePartyDetailsPropertiesError()
    extends RuntimeException("MODIFYING_AN_UNMODIFIABLE_PARTY_DETAILS_PROPERTY_ERROR")
    with NoStackTrace
