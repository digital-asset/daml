// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.v2.version_service.{
  GetLedgerApiVersionResponse,
  OffsetCheckpointFeature,
  PartyManagementFeature,
  UserManagementFeature,
}

final case class Features(
    userManagement: UserManagementFeature,
    partyManagement: PartyManagementFeature,
    staticTime: Boolean,
    offsetCheckpoint: OffsetCheckpointFeature,
)

object Features {
  def fromApiVersionResponse(response: GetLedgerApiVersionResponse): Features = {
    val features = response.getFeatures
    val experimental = features.getExperimental

    Features(
      userManagement = features.getUserManagement,
      partyManagement = features.getPartyManagement,
      staticTime = experimental.getStaticTime.supported,
      offsetCheckpoint = features.getOffsetCheckpoint,
    )
  }
}
