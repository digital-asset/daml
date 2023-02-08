// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.client.binding.Primitive
import scalaz.syntax.tag._

case class AllocatedPartySet(
    mainPartyNamePrefix: String,
    parties: List[Primitive.Party],
) {
  {
    val offenders = parties.iterator.filterNot(_.unwrap.startsWith(mainPartyNamePrefix)).toList
    require(
      offenders.isEmpty,
      s"All party names in party-set '$mainPartyNamePrefix' must start with prefix $mainPartyNamePrefix. Found offenders: $offenders",
    )
  }
}
