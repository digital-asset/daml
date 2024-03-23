// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.v1.experimental_features.ExperimentalCommitterEventLog.CommitterEventLogType.CENTRALIZED
import com.daml.ledger.api.v1.experimental_features.{
  CommandDeduplicationFeatures,
  ExperimentalCommitterEventLog,
  ExperimentalContractIds,
}
import com.daml.ledger.api.v1.version_service.{GetLedgerApiVersionResponse, UserManagementFeature}

final case class Features(
    userManagement: UserManagementFeature,
    staticTime: Boolean,
    commandDeduplicationFeatures: CommandDeduplicationFeatures,
    optionalLedgerId: Boolean = false,
    contractIds: ExperimentalContractIds,
    committerEventLog: ExperimentalCommitterEventLog,
    explicitDisclosure: Boolean = false,
    userAndPartyLocalMetadataExtensions: Boolean = false,
    acsActiveAtOffsetFeature: Boolean = false,
)

object Features {
  val defaultFeatures: Features = Features(
    userManagement = UserManagementFeature.defaultInstance,
    staticTime = false,
    commandDeduplicationFeatures = CommandDeduplicationFeatures.defaultInstance,
    contractIds = ExperimentalContractIds.defaultInstance,
    committerEventLog = ExperimentalCommitterEventLog.of(eventLogType = CENTRALIZED),
  )

  def fromApiVersionResponse(response: GetLedgerApiVersionResponse): Features = {
    val features = response.getFeatures
    val experimental = features.getExperimental

    Features(
      userManagement = features.getUserManagement,
      staticTime = experimental.getStaticTime.supported,
      commandDeduplicationFeatures = experimental.getCommandDeduplication,
      optionalLedgerId = experimental.optionalLedgerId.isDefined,
      contractIds = experimental.getContractIds,
      committerEventLog = experimental.getCommitterEventLog,
      explicitDisclosure = experimental.getExplicitDisclosure.supported,
      userAndPartyLocalMetadataExtensions =
        experimental.getUserAndPartyLocalMetadataExtensions.supported,
      acsActiveAtOffsetFeature = experimental.getAcsActiveAtOffset.supported,
    )
  }
}
