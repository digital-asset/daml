// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref.Location

final case class IncompleteTransaction(
    transaction: GenTransaction,
    locationInfo: Map[NodeId, Location],
)
