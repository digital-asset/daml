// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import com.daml.lf.data.Ref.Location

/** Top-level speedy definition.
  *
  * The computation of the definition is cached to avoid expensive
  * re-evaluation of complex definitions. When evaluating SEVal we
  * further memoize the cached value in SEVal to avoid the definition
  * lookup.
  */
final case class SDefinition(
    body: SExpr,
) {
  private var _cached: Option[(SValue, List[Location])] = None
  private[speedy] def cached: Option[(SValue, List[Location])] = _cached
  private[speedy] def setCached(sValue: SValue, stack_trace: List[Location]): Unit =
    _cached = Some((sValue, stack_trace))
}
