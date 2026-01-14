// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.topology.transaction.SynchronizerTrustCertificate.ParticipantTopologyFeatureFlag

object ParticipantProtocolFeatureFlags {

  /** Feature flags supported by participant node for each PV
    */
  val supportedFeatureFlagsByPV: Map[ProtocolVersion, Set[ParticipantTopologyFeatureFlag]] = Map(
    ProtocolVersion.v34 -> Set.empty
  )
}
