// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import com.digitalasset.canton.participant.admin.data.{
  RepairContract,
  RepresentativePackageIdOverride,
}

// TODO(#27872): Implement complete assigning logic
class AssignRepresentativePackageIds(
    representativePackageIdOverride: RepresentativePackageIdOverride
) {
  def apply(contracts: List[RepairContract]): List[RepairContract] =
    // TODO(#27872): Double the memory requirements. Prefer streaming
    contracts.map(repairContract =>
      representativePackageIdOverride.contractOverride
        .get(repairContract.contract.contractId)
        .map(newRpId => repairContract.copy(representativePackageId = newRpId))
        .getOrElse(repairContract)
    )
}
