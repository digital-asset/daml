// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.topology.transaction.ParticipantDomainLimits as ParticipantDomainLimitsInternal
import io.scalaland.chimney.dsl.*

final case class ParticipantDomainLimits(
    confirmationRequestsMaxRate: NonNegativeInt
) {
  def toInternal: ParticipantDomainLimitsInternal =
    this.transformInto[ParticipantDomainLimitsInternal]

}
