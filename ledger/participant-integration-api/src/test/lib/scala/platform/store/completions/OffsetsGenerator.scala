// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.completions

import com.daml.ledger.participant.state.v1.Offset

object OffsetsGenerator {
  def genOffset(value: Int): Offset = Offset.fromByteArray(Seq(value.toByte).toArray)
}
