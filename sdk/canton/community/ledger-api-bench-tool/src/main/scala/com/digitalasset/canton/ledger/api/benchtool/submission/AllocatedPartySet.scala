// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.submission

import com.daml.ledger.javaapi.data.Party

final case class AllocatedPartySet(
    mainPartyNamePrefix: String,
    parties: List[Party],
) {
  {
    val offenders = parties.iterator.filterNot(_.getValue.startsWith(mainPartyNamePrefix)).toList
    require(
      offenders.isEmpty,
      s"All party names in party-set '$mainPartyNamePrefix' must start with prefix $mainPartyNamePrefix. Found offenders: $offenders",
    )
  }
}
