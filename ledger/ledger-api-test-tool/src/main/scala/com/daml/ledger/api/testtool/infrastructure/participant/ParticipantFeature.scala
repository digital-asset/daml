// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.v1.version_service.{
  CommandDeduplicationFeatures,
  GetLedgerApiVersionResponse,
}

final case class Features(
    selfServiceErrorCodes: Boolean = false,
    userManagement: Boolean = false,
    commandDeduplicationFeatures: CommandDeduplicationFeatures,
    staticTime: Boolean = false,
)

object Features {
  val defaultFeatures =
    Features(commandDeduplicationFeatures = CommandDeduplicationFeatures.defaultInstance)

  def fromApiVersionResponse(response: GetLedgerApiVersionResponse): Features = {
    val features = response.features
    val experimental = features.flatMap(_.experimental)

    Features(
      selfServiceErrorCodes = experimental.flatMap(_.selfServiceErrorCodes).isDefined,
      userManagement = features.flatMap(_.userManagement).isDefined,
      commandDeduplicationFeatures = response.getFeatures.getCommandDeduplication,
      staticTime = experimental.flatMap(_.staticTime).isDefined,
    )
  }
}
