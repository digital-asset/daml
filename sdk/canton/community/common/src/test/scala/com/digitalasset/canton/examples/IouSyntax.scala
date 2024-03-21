// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.examples.java

import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.topology.PartyId

import scala.jdk.CollectionConverters.*

object IouSyntax {
  def testIou(payer: PartyId, owner: PartyId, amount: Double = 100.0): iou.Iou =
    new iou.Iou(
      payer.toProtoPrimitive,
      owner.toProtoPrimitive,
      new iou.Amount(amount.toBigDecimal, "USD"),
      List.empty.asJava,
    )
}
