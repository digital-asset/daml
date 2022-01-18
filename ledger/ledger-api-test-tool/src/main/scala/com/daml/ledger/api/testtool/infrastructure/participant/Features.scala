// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.v1.experimental_features.CommandDeduplicationFeatures
import com.daml.ledger.api.v1.version_service.GetLedgerApiVersionResponse

final case class Features(
    selfServiceErrorCodes: Boolean,
    userManagement: Boolean,
    commandDeduplicationFeatures: CommandDeduplicationFeatures,
    staticTime: Boolean,
)

object Features {
  val defaultFeatures: Features = Features(
    selfServiceErrorCodes = false,
    userManagement = false,
    commandDeduplicationFeatures = CommandDeduplicationFeatures.defaultInstance,
    staticTime = false,
  )

  def fromApiVersionResponse(response: GetLedgerApiVersionResponse): Features = {
    val features = response.getFeatures
    val experimental = features.getExperimental

    Features(
      selfServiceErrorCodes = experimental.selfServiceErrorCodes.isDefined,
      userManagement = features.getUserManagement.supported,
      staticTime = experimental.getStaticTime.supported,
      commandDeduplicationFeatures = experimental.getCommandDeduplication,
    )
  }
}
