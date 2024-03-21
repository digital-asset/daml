// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref.Location

final case class IncompleteTransaction(
    transaction: Transaction,
    locationInfo: Map[NodeId, Location],
)
