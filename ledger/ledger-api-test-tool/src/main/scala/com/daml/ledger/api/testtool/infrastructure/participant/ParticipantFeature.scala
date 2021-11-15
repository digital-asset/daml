// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.v1.version_service.GetLedgerApiVersionResponse

object Features {
  def fromApiVersionResponse(request: GetLedgerApiVersionResponse): Features = {
    val selfServiceErrorCodesFeature = for {
      features <- request.features
      experimental <- features.experimental
      _ <- experimental.selfServiceErrorCodes
    } yield SelfServiceErrorCodes

    Features(selfServiceErrorCodesFeature.toList)
  }
}
case class Features(features: Seq[Feature]) {
  val selfServiceErrorCodes: Boolean = SelfServiceErrorCodes.enabled(features)
}

sealed trait Feature

sealed trait ExperimentalFeature extends Feature

case object SelfServiceErrorCodes extends ExperimentalFeature {
  def enabled(features: Seq[Feature]): Boolean = features.contains(SelfServiceErrorCodes)
}
