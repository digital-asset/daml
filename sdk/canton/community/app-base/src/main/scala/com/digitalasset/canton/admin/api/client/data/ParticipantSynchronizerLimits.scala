// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.topology.transaction.ParticipantSynchronizerLimits as ParticipantSynchronizerLimitsInternal
import io.scalaland.chimney.dsl.*

final case class ParticipantSynchronizerLimits(
    confirmationRequestsMaxRate: NonNegativeInt
) {
  def toInternal: ParticipantSynchronizerLimitsInternal =
    this.transformInto[ParticipantSynchronizerLimitsInternal]

}
