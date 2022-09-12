// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

// TODO um-for-hub: Get field names from files generated from proto files instead of hardcoding them
object FieldNames {
  object UpdateUserRequest {
    val user = "user"
  }
  object User {
    val primaryParty = "primary_party"
    val isDeactivated = "is_deactivated"
    val metadata = "metadata"
  }
  object Metadata {
    val annotations = "annotations"
  }

  object UpdatePartyDetailsRequest {
    val partyDetails = "party_details"
  }
  object PartyDetails {
    val localMetadata = "local_metadata"
  }
}
