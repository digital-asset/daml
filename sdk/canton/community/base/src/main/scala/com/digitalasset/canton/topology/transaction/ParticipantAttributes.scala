// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.digitalasset.canton.data.CantonTimestamp

final case class ParticipantAttributes(
    permission: ParticipantPermission,
    loginAfter: Option[CantonTimestamp] = None,
    features: Seq[SynchronizerTrustCertificate.ParticipantTopologyFeatureFlag] = Seq.empty,
    onboarding: Boolean = false,
) {
  def canConfirm: Boolean = permission.canConfirm && !onboarding
}
