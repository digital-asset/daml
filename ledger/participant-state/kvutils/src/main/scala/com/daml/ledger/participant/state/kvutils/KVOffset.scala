// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.v1.Offset

object KVOffset {
  def onlyKeepSignificantIndex(offset: Offset): Offset = {
    Offset.assertFromString(offset.value.takeWhile(_ != '-'))
  }

  def addSubIndex(offset: Offset, index: Long): Offset = {
    Offset.assertFromString(offset.value + "-%05d".format(index))
  }
}
