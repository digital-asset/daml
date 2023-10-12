// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.examples

import com.digitalasset.canton.topology.PartyId

object IouSyntax {
  def testIou(payer: PartyId, owner: PartyId): Iou.Iou =
    Iou.Iou(
      payer = payer.toPrim,
      owner = owner.toPrim,
      amount = Iou.Amount(100, "USD"),
      viewers = List.empty,
    )
}
