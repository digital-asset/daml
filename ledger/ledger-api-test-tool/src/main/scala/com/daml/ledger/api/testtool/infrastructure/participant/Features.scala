// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.v1.experimental_features.{
  CommandDeduplicationFeatures,
  ExperimentalContractIds,
}
import com.daml.ledger.api.v1.version_service.GetLedgerApiVersionResponse

final case class Features(
    userManagement: Boolean,
    selfServiceErrorCodes: Boolean,
    staticTime: Boolean,
    commandDeduplicationFeatures: CommandDeduplicationFeatures,
    optionalLedgerId: Boolean = false,
    contractIds: ExperimentalContractIds,
)

object Features {
  val defaultFeatures: Features = Features(
    userManagement = false,
    selfServiceErrorCodes = false,
    staticTime = false,
    commandDeduplicationFeatures = CommandDeduplicationFeatures.defaultInstance,
    contractIds = ExperimentalContractIds.defaultInstance,
  )

  def fromApiVersionResponse(response: GetLedgerApiVersionResponse): Features = {
    val features = response.getFeatures
    val experimental = features.getExperimental

    Features(
      userManagement = features.getUserManagement.supported,
      selfServiceErrorCodes = experimental.selfServiceErrorCodes.isDefined,
      staticTime = experimental.getStaticTime.supported,
      commandDeduplicationFeatures = experimental.getCommandDeduplication,
      optionalLedgerId = experimental.optionalLedgerId.isDefined,
      contractIds = experimental.getContractIds,
    )
  }
}
