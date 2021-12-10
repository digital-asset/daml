// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.v1.version_service.GetLedgerApiVersionResponse

final case class Features(selfServiceErrorCodes: Boolean = false, userManagement: Boolean = false)

object Features {
  val noFeatures = Features()

  def fromApiVersionResponse(request: GetLedgerApiVersionResponse): Features = {
    val features = request.features
    val experimental = features.flatMap(_.experimental)

    Features(
      selfServiceErrorCodes = experimental.flatMap(_.selfServiceErrorCodes).isDefined,
      userManagement = features.flatMap(_.userManagement).isDefined,
    )
  }
}
