// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

/** Ledger API topology-aware package selection specific configurations
  *
  * @param enabled
  *   whether to enable topology-aware package selection in command interpretation
  */
final case class TopologyAwarePackageSelectionConfig(
    enabled: Boolean = false
)

object TopologyAwarePackageSelectionConfig {
  lazy val Default: TopologyAwarePackageSelectionConfig = TopologyAwarePackageSelectionConfig()
}
